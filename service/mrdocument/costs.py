"""
Cost tracking module for Anthropic API usage.

Tracks Anthropic token usage per model and calculates costs based on pricing config.
Uses an in-memory queue with periodic flush to avoid file locking issues.
"""

import asyncio
import json
import logging
import os
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from queue import Queue, Empty
from threading import Thread
from typing import Optional

import yaml

logger = logging.getLogger(__name__)


@dataclass
class AnthropicUsage:
    """Record of Anthropic API usage."""
    model: str
    input_tokens: int
    output_tokens: int
    user_dir: Path
    count_document: bool = False  # If True, increment document counter


class CostTracker:
    """
    Tracks API costs with in-memory queue and periodic flush.
    
    Each service (mrdocument, stt) should have its own CostTracker instance
    writing to a separate file to avoid cross-container conflicts.
    """
    
    def __init__(
        self,
        filename: str = "mrdocument_costs.json",
        flush_interval: float = 30.0,
        pricing_config_path: Optional[Path] = None,
    ):
        """
        Initialize cost tracker.
        
        Args:
            filename: Name of the costs file (created in each user's dir)
            flush_interval: Seconds between flushes to disk
            pricing_config_path: Path to pricing.yaml config
        """
        self.filename = filename
        self.flush_interval = flush_interval
        self.queue: Queue = Queue()
        self._running = False
        self._flush_thread: Optional[Thread] = None
        
        # Load pricing config
        if pricing_config_path is None:
            pricing_config_path = Path(__file__).parent / "pricing.yaml"
        self.pricing = self._load_pricing(pricing_config_path)
        
        logger.info("CostTracker initialized: file=%s, flush_interval=%.1fs", filename, flush_interval)
    
    def _load_pricing(self, path: Path) -> dict:
        """Load pricing configuration from YAML."""
        if not path.exists():
            logger.warning("Pricing config not found at %s, using empty pricing", path)
            return {"anthropic": {}}
        
        try:
            with open(path) as f:
                return yaml.safe_load(f) or {"anthropic": {}}
        except Exception as e:
            logger.error("Failed to load pricing config: %s", e)
            return {"anthropic": {}}
    
    def start(self) -> None:
        """Start the background flush thread."""
        if self._running:
            return
        
        self._running = True
        self._flush_thread = Thread(target=self._flush_loop, daemon=True)
        self._flush_thread.start()
        logger.info("CostTracker flush thread started")
    
    def stop(self) -> None:
        """Stop the flush thread and perform final flush."""
        if not self._running:
            return
        
        self._running = False
        if self._flush_thread:
            self._flush_thread.join(timeout=5.0)
        
        # Final flush
        self._flush_all()
        logger.info("CostTracker stopped")
    
    def record_anthropic(
        self,
        model: str,
        input_tokens: int,
        output_tokens: int,
        user_dir: Path,
        count_document: bool = False,
    ) -> None:
        """
        Record Anthropic API usage.
        
        Args:
            model: Model name (e.g., "claude-sonnet-4-5")
            input_tokens: Number of input tokens
            output_tokens: Number of output tokens
            user_dir: User's directory where costs.json will be stored
            count_document: If True, increment the document counter
        """
        usage = AnthropicUsage(
            model=model,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            user_dir=user_dir,
            count_document=count_document,
        )
        self.queue.put(usage)
        logger.debug(
            "Queued Anthropic usage: model=%s, in=%d, out=%d, user=%s, count_doc=%s",
            model, input_tokens, output_tokens, user_dir.name, count_document
        )
    
    def _flush_loop(self) -> None:
        """Background loop that periodically flushes the queue."""
        while self._running:
            try:
                asyncio.run(asyncio.sleep(self.flush_interval))
            except Exception:
                pass
            
            if self._running:
                self._flush_all()
    
    def _flush_all(self) -> None:
        """Flush all queued usage records to disk."""
        # Collect all pending records grouped by user_dir
        records_by_user: dict[Path, list] = {}
        
        while True:
            try:
                usage = self.queue.get_nowait()
                user_dir = usage.user_dir
                if user_dir not in records_by_user:
                    records_by_user[user_dir] = []
                records_by_user[user_dir].append(usage)
            except Empty:
                break
        
        if not records_by_user:
            return
        
        # Write to each user's costs file
        today = date.today().isoformat()
        
        for user_dir, records in records_by_user.items():
            try:
                self._write_user_costs(user_dir, today, records)
            except Exception as e:
                logger.error("Failed to write costs for %s: %s", user_dir.name, e)
    
    def _write_user_costs(self, user_dir: Path, today: str, records: list) -> None:
        """Write usage records to a user's costs file."""
        # Extract username from user_dir (e.g., /sync/alice/mrdocument -> alice)
        # Go up to find the user folder under /sync
        username = None
        for parent in [user_dir] + list(user_dir.parents):
            if parent.parent.name == "sync" or str(parent.parent) == "/sync":
                username = parent.name
                break
        
        if not username:
            # Fallback: use the parent directory name
            username = user_dir.parent.name if user_dir.parent.name != "sync" else user_dir.name
        
        # Write to /costs/{username}/
        costs_dir = Path("/costs") / username
        costs_dir.mkdir(parents=True, exist_ok=True)
        costs_path = costs_dir / self.filename
        
        # Load existing data
        if costs_path.exists():
            try:
                with open(costs_path) as f:
                    data = json.load(f)
            except (json.JSONDecodeError, Exception):
                data = {}
        else:
            data = {}
        
        # Ensure today's entry exists
        if today not in data:
            data[today] = {"anthropic": {}}

        day_data = data[today]

        # Aggregate records
        for usage in records:
            if isinstance(usage, AnthropicUsage):
                if "anthropic" not in day_data:
                    day_data["anthropic"] = {}
                
                # Get or create model-specific entry
                model_key = usage.model
                if model_key not in day_data["anthropic"]:
                    day_data["anthropic"][model_key] = {
                        "input_tokens": 0, "output_tokens": 0, "cost": 0.0, "documents": 0
                    }
                model_data = day_data["anthropic"][model_key]
                
                model_data["input_tokens"] += usage.input_tokens
                model_data["output_tokens"] += usage.output_tokens
                if usage.count_document:
                    model_data["documents"] += 1
                
                # Calculate cost
                cost = self._calculate_anthropic_cost(
                    usage.model, usage.input_tokens, usage.output_tokens
                )
                model_data["cost"] += cost
        
        # Round costs to avoid floating point artifacts
        if "anthropic" in day_data:
            for model_data in day_data["anthropic"].values():
                model_data["cost"] = round(model_data["cost"], 6)
        
        # Recalculate totals across all days
        total: dict = {"anthropic": {}}
        for date_key, date_data in data.items():
            if date_key == "total":
                continue
            
            # Aggregate anthropic per model
            if "anthropic" in date_data:
                for model_name, model_stats in date_data["anthropic"].items():
                    if model_name not in total["anthropic"]:
                        total["anthropic"][model_name] = {
                            "input_tokens": 0, "output_tokens": 0, "cost": 0.0, "documents": 0
                        }
                    total["anthropic"][model_name]["input_tokens"] += model_stats.get("input_tokens", 0)
                    total["anthropic"][model_name]["output_tokens"] += model_stats.get("output_tokens", 0)
                    total["anthropic"][model_name]["cost"] += model_stats.get("cost", 0.0)
                    total["anthropic"][model_name]["documents"] += model_stats.get("documents", 0)
        
        # Calculate cost per document and round costs
        for model_name, model_stats in total["anthropic"].items():
            model_stats["cost"] = round(model_stats["cost"], 6)
            if model_stats["documents"] > 0:
                model_stats["cost_per_document"] = round(model_stats["cost"] / model_stats["documents"], 6)
            else:
                model_stats["cost_per_document"] = 0.0
        
        data["total"] = total
        
        # Write atomically via temp file
        temp_path = costs_path.with_suffix(".tmp")
        with open(temp_path, "w") as f:
            json.dump(data, f, indent=2)
        temp_path.rename(costs_path)
        
        logger.debug("Flushed %d records to %s", len(records), costs_path)
    
    def _calculate_anthropic_cost(self, model: str, input_tokens: int, output_tokens: int) -> float:
        """Calculate cost for Anthropic usage."""
        pricing = self.pricing.get("anthropic", {}).get(model)
        if not pricing:
            logger.warning("No pricing found for Anthropic model: %s", model)
            return 0.0
        
        input_cost = (input_tokens / 1_000_000) * pricing.get("input_per_1m", 0)
        output_cost = (output_tokens / 1_000_000) * pricing.get("output_per_1m", 0)
        return input_cost + output_cost


# Global instance for MrDocument
_cost_tracker: Optional[CostTracker] = None


def get_cost_tracker() -> CostTracker:
    """Get the global cost tracker instance, creating if needed."""
    global _cost_tracker
    if _cost_tracker is None:
        _cost_tracker = CostTracker()
        _cost_tracker.start()
    return _cost_tracker


def shutdown_cost_tracker() -> None:
    """Shutdown the global cost tracker."""
    global _cost_tracker
    if _cost_tracker is not None:
        _cost_tracker.stop()
        _cost_tracker = None
