"""REST server for MrDocument service."""

import asyncio
import base64
import copy
from datetime import date
import json
import logging
import os
from pathlib import Path
from typing import Any, Optional

from aiohttp import web

from .ai import AiClient, AiError, ConfigurationError, ContextConfig, DocumentMetadata, FieldConfig, load_config
from .costs import get_cost_tracker, shutdown_cost_tracker
from .docx_utils import embed_metadata_in_docx, extract_text_from_docx
from .eml import eml_to_pdf
from .html_convert import html_to_pdf
from .image import image_to_pdf, is_supported_image
from .rtf_convert import rtf_to_pdf
from .ocr import OcrClient, OcrError
from .pdf import embed_metadata
from .transcript import (
    strip_words_from_json,
    create_text_content,
    correct_transcript_json,
)

logger = logging.getLogger(__name__)


def parse_field_config(data: Any) -> Optional[FieldConfig]:
    """
    Parse a field configuration from JSON data.

    Args:
        data: Parsed JSON data (dict or None)

    Returns:
        FieldConfig or None if data is None/empty or missing required instructions
    """
    if data is None:
        return None

    if not isinstance(data, dict):
        return None

    result: FieldConfig = {}

    # Parse instructions (REQUIRED)
    if "instructions" in data and isinstance(data["instructions"], str):
        result["instructions"] = data["instructions"]
    else:
        # instructions is required
        return None

    # Parse candidates (list of strings or objects with name/short/clues, or null)
    if "candidates" in data:
        if data["candidates"] is None:
            result["candidates"] = None
        elif isinstance(data["candidates"], list):
            result["candidates"] = data["candidates"]

    # Parse blacklist (always list of strings)
    if "blacklist" in data and isinstance(data["blacklist"], list):
        result["blacklist"] = data["blacklist"]

    # Parse allow_new_candidates (boolean, default True)
    if "allow_new_candidates" in data:
        result["allow_new_candidates"] = bool(data["allow_new_candidates"])

    # Parse include_in_context_determination (boolean, default False)
    if "include_in_context_determination" in data:
        result["include_in_context_determination"] = bool(data["include_in_context_determination"])

    return result


def parse_contexts(data: Any) -> Optional[list[ContextConfig]]:
    """
    Parse contexts from JSON data.

    Each context has:
    - name: Context name (required)
    - description: Context description (required)
    - filename: Filename pattern (required)
    - fields: Dict of field configurations (required)

    Args:
        data: Parsed JSON data (list of dicts)

    Returns:
        List of ContextConfig or None if data is None/empty/invalid
    """
    if data is None:
        return None

    if not isinstance(data, list):
        return None

    if not data:
        return None

    contexts: list[ContextConfig] = []
    for item in data:
        if not isinstance(item, dict):
            continue

        # Required fields
        if "name" not in item or "description" not in item:
            continue

        # Handle filename: optional, with conditional list support
        filename_value = item.get("filename")
        if isinstance(filename_value, list):
            default_pattern = None
            for entry in filename_value:
                if isinstance(entry, dict) and "pattern" in entry and "match" not in entry:
                    default_pattern = entry["pattern"]
                    break
            if default_pattern:
                filename_value = default_pattern
            else:
                logger.error(
                    "Context '%s': filename is a conditional list with no default pattern",
                    item.get("name"),
                )
                filename_value = None
        if not filename_value:
            filename_value = "{context}-{date}"

        context: ContextConfig = {
            "name": item["name"],
            "description": item["description"],
            "filename": filename_value,
        }

        # Parse optional audio_filename pattern
        audio_filename = item.get("audio_filename")
        if isinstance(audio_filename, str) and audio_filename:
            context["audio_filename"] = audio_filename

        # Parse optional transcription_keyterms at context level
        if "transcription_keyterms" in item and isinstance(item["transcription_keyterms"], list):
            context["transcription_keyterms"] = [str(k) for k in item["transcription_keyterms"] if k]

        # Parse field configurations from 'fields' object (required)
        if "fields" in item and isinstance(item["fields"], dict):
            fields: dict[str, FieldConfig] = {}
            for field_name, field_data in item["fields"].items():
                field_config = parse_field_config(field_data)
                if field_config:
                    fields[field_name] = field_config
            context["fields"] = fields
        else:
            # fields is required
            continue

        contexts.append(context)

    return contexts if contexts else None


class MrDocumentServer:
    """MrDocument REST API server."""

    def __init__(
        self,
        ocr_url: str,
        anthropic_api_key: str,
        anthropic_model: Optional[str] = None,
    ):
        """
        Initialize the server.

        Args:
            ocr_url: URL of the OCR service
            anthropic_api_key: Anthropic API key
            anthropic_model: Model to use (overrides config.yaml if provided)
        """
        self.ocr_client = OcrClient(ocr_url)
        self.ai_client = AiClient(anthropic_api_key, anthropic_model)

        # Load config
        config = load_config()

        # Transcript correction config
        correction_config = config.get("transcript_correction", {})
        self.correction_model = correction_config.get("model", "claude-sonnet-4-20250514")
        self.correction_extended_thinking = correction_config.get("extended_thinking", True)
        self.correction_thinking_budget = correction_config.get("thinking_budget", 50000)
        self.correction_use_batch = correction_config.get("use_batch", True)

        logger.info("MrDocumentServer initialized with OCR URL: %s", ocr_url)
        logger.info(
            "Transcript correction: model=%s, extended_thinking=%s, budget=%d, use_batch=%s",
            self.correction_model,
            self.correction_extended_thinking,
            self.correction_thinking_budget,
            self.correction_use_batch,
        )

    async def health(self, request: web.Request) -> web.Response:
        """Health check endpoint."""
        ocr_healthy = await self.ocr_client.health_check()
        return web.json_response(
            {
                "status": "healthy" if ocr_healthy else "degraded",
                "service": "mrdocument",
                "ocr_service": "healthy" if ocr_healthy else "unhealthy",
            }
        )

    async def process_document(self, request: web.Request) -> web.Response:
        """
        Process a document (PDF, EML, HTML, DOCX, or image).

        Accepts multipart/form-data with:
        - file: Document file to process
        - language: OCR language (optional, default: eng)
        - primary_language: Output language for AI responses (optional)
        - contexts: JSON array of context configurations (required)

        Each context in the array has:
        - name: Context name (required)
        - description: Context description (required)
        - filename: Filename pattern with {field} placeholders (required)
        - fields: Object mapping field names to configurations (required)
          Each field configuration has:
          - instructions: Semantic description of this field (required)
          - candidates: List of allowed values or null (optional)
          - blacklist: List of forbidden values (optional)
          - allow_new_candidates: Boolean (default false)

        Returns JSON with:
        - filename: Suggested filename based on metadata
        - pdf/docx: Base64-encoded processed document
        - metadata: Extracted metadata fields including context
        """
        content_type = request.content_type
        if not content_type or not content_type.startswith("multipart/"):
            logger.warning("Invalid content type: %s", content_type)
            return web.json_response({"error": "multipart/form-data required"}, status=400)

        reader = await request.multipart()

        file_bytes: Optional[bytes] = None
        filename: str = "document.pdf"
        language: str = "eng"
        primary_language: Optional[str] = None
        contexts: Optional[list[ContextConfig]] = None
        user_dir: Optional[Path] = None
        locked_fields: Optional[dict[str, dict[str, Any]]] = None

        # Read multipart fields
        async for field in reader:
            if field.name == "file":
                filename = field.filename or "document.pdf"
                file_bytes = await field.read()
            elif field.name == "language":
                language = (await field.read()).decode("utf-8")
            elif field.name == "primary_language":
                primary_language = (await field.read()).decode("utf-8")
            elif field.name == "contexts":
                try:
                    contexts_data = (await field.read()).decode("utf-8")
                    contexts = parse_contexts(json.loads(contexts_data))
                except (json.JSONDecodeError, UnicodeDecodeError) as e:
                    logger.warning("Invalid JSON in 'contexts' field: %s", e)
                    return web.json_response(
                        {"error": "Invalid JSON in 'contexts' field"}, status=400
                    )
            elif field.name == "locked_fields":
                try:
                    locked_fields_data = (await field.read()).decode("utf-8")
                    locked_fields = json.loads(locked_fields_data)
                except (json.JSONDecodeError, UnicodeDecodeError) as e:
                    logger.warning("Invalid JSON in 'locked_fields' field: %s", e)
                    return web.json_response(
                        {"error": "Invalid JSON in 'locked_fields' field"}, status=400
                    )
            elif field.name == "user_dir":
                user_dir_str = (await field.read()).decode("utf-8")
                if user_dir_str:
                    user_dir = Path(user_dir_str)

        if file_bytes is None:
            logger.warning("No file provided in request")
            return web.json_response({"error": "No file provided"}, status=400)

        if not contexts:
            logger.warning("No contexts provided in request")
            return web.json_response(
                {"error": "contexts field is required with at least one context"},
                status=400,
            )

        filename_lower = filename.lower()
        is_pdf = filename_lower.endswith(".pdf")
        is_eml = filename_lower.endswith(".eml")
        is_html = filename_lower.endswith(".html") or filename_lower.endswith(".htm")
        is_docx = filename_lower.endswith(".docx")
        is_text = filename_lower.endswith(".txt") or filename_lower.endswith(".md")
        is_rtf = filename_lower.endswith(".rtf")
        is_image = is_supported_image(filename)

        if not is_pdf and not is_eml and not is_html and not is_docx and not is_text and not is_rtf and not is_image:
            logger.warning("Unsupported file type submitted: %s", filename)
            return web.json_response(
                {"error": "Only PDF, EML, HTML, DOCX, RTF, TXT, MD, and image files are supported"},
                status=400,
            )

        # Handle text files separately - no OCR needed, just classification
        if is_text:
            return await self._process_text(
                file_bytes, filename, primary_language, contexts, user_dir, locked_fields
            )

        # Handle DOCX separately - no OCR needed
        if is_docx:
            return await self._process_docx(
                file_bytes, filename, primary_language, contexts, user_dir, locked_fields
            )

        # Convert to PDF if needed for OCR processing
        if is_eml:
            logger.info("Converting EML to PDF: %s (%d bytes)", filename, len(file_bytes))
            try:
                pdf_bytes = eml_to_pdf(file_bytes)
                filename = filename[:-4] + ".pdf"
            except Exception as e:
                logger.error("EML to PDF conversion failed for %s: %s", filename, e)
                return web.json_response(
                    {"error": f"EML conversion failed: {e}"}, status=400
                )
        elif is_html:
            logger.info("Converting HTML to PDF: %s (%d bytes)", filename, len(file_bytes))
            try:
                pdf_bytes = html_to_pdf(file_bytes, filename)
                filename = os.path.splitext(filename)[0] + ".pdf"
            except Exception as e:
                logger.error("HTML to PDF conversion failed for %s: %s", filename, e)
                return web.json_response(
                    {"error": f"HTML conversion failed: {e}"}, status=400
                )
        elif is_rtf:
            logger.info("Converting RTF to PDF: %s (%d bytes)", filename, len(file_bytes))
            try:
                pdf_bytes = rtf_to_pdf(file_bytes, filename)
                filename = os.path.splitext(filename)[0] + ".pdf"
            except Exception as e:
                logger.error("RTF to PDF conversion failed for %s: %s", filename, e)
                return web.json_response(
                    {"error": f"RTF conversion failed: {e}"}, status=400
                )
        elif is_image:
            logger.info("Converting image to PDF: %s (%d bytes)", filename, len(file_bytes))
            try:
                pdf_bytes = image_to_pdf(file_bytes, filename)
                filename = os.path.splitext(filename)[0] + ".pdf"
            except Exception as e:
                logger.error("Image to PDF conversion failed for %s: %s", filename, e)
                return web.json_response(
                    {"error": f"Image conversion failed: {e}"}, status=400
                )
        else:
            pdf_bytes = file_bytes

        logger.info(
            "Processing document: %s (%d bytes), language=%s, contexts=%d",
            filename,
            len(pdf_bytes),
            language,
            len(contexts),
        )

        try:
            # Step 1: OCR the document
            logger.debug("Starting OCR for %s", filename)
            ocr_result = await self.ocr_client.process_pdf(pdf_bytes, filename, language)
            logger.debug("OCR complete, extracted %d chars of text", len(ocr_result.text))

            # Step 2: Extract metadata using AI (two-pass with context determination)
            logger.debug("Starting AI metadata extraction")
            metadata, filename_pattern = await self.ai_client.extract_metadata(
                ocr_result.text,
                contexts=contexts,
                primary_language=primary_language,
                filename=filename,
                user_dir=user_dir,
                locked_fields=locked_fields,
            )

            # Step 3: Embed metadata into PDF (using legacy accessors for standard fields)
            logger.debug("Embedding metadata into PDF")
            pdf_with_metadata = embed_metadata(
                ocr_result.pdf_bytes,
                doc_type=metadata.doc_type,
                doc_date=metadata.date,
                sender=metadata.sender,
                topic=metadata.topic,
                subject=metadata.subject,
                keywords=metadata.keywords,
            )

            # Step 4: Generate filename from metadata using context's pattern
            suggested_filename = metadata.to_filename(filename_pattern, source_filename=filename)
            logger.info(
                "Document processed successfully: %s -> %s (context: %s)",
                filename,
                suggested_filename,
                metadata.context,
            )

            # Build metadata response with all fields
            metadata_response = {
                "context": metadata.context,
                "date": metadata.date.isoformat() if metadata.date else None,
            }
            # Add all extracted fields
            metadata_response.update(metadata.fields)
            
            response_data = {
                "filename": suggested_filename,
                "pdf": base64.b64encode(pdf_with_metadata).decode("utf-8"),
                "metadata": metadata_response,
            }

            # Include new clues if any were suggested
            if metadata.new_clues:
                response_data["new_clues"] = {
                    field: {"value": value, "clue": clue}
                    for field, (value, clue) in metadata.new_clues.items()
                }

            # Include signature warning if a digital signature was invalidated
            if ocr_result.signature_invalidated:
                response_data["signature_invalidated"] = True
                logger.warning("Digital signature was invalidated for %s", filename)

            return web.json_response(response_data)

        except ConfigurationError as e:
            logger.error("Configuration error for %s: %s", filename, e)
            return web.json_response({"error": f"Configuration error: {e}"}, status=400)
        except OcrError as e:
            logger.error("OCR failed for %s: %s", filename, e)
            return web.json_response({"error": f"OCR failed: {e}"}, status=502)
        except AiError as e:
            logger.error("AI processing failed for %s: %s", filename, e)
            return web.json_response({"error": f"AI processing failed: {e}"}, status=502)
        except Exception as e:
            logger.exception("Internal error processing %s", filename)
            return web.json_response({"error": f"Internal error: {e}"}, status=500)

    async def _process_docx(
        self,
        file_bytes: bytes,
        filename: str,
        primary_language: Optional[str],
        contexts: list[ContextConfig],
        user_dir: Optional[Path] = None,
        locked_fields: Optional[dict[str, dict[str, Any]]] = None,
    ) -> web.Response:
        """
        Process a DOCX file - extract text and metadata without OCR.
        """
        logger.info(
            "Processing DOCX: %s (%d bytes), contexts=%d",
            filename,
            len(file_bytes),
            len(contexts),
        )

        try:
            # Step 1: Extract text from DOCX
            logger.debug("Extracting text from DOCX: %s", filename)
            try:
                text = extract_text_from_docx(file_bytes)
            except ValueError as e:
                logger.error("Failed to extract text from DOCX %s: %s", filename, e)
                return web.json_response(
                    {"error": f"DOCX extraction failed: {e}"}, status=400
                )

            if not text.strip():
                logger.warning("DOCX %s has no extractable text", filename)
                return web.json_response(
                    {"error": "DOCX file contains no extractable text"}, status=400
                )

            # Step 2: Extract metadata using AI
            logger.debug("Starting AI metadata extraction for DOCX")
            metadata, filename_pattern = await self.ai_client.extract_metadata(
                text,
                contexts=contexts,
                primary_language=primary_language,
                filename=filename,
                user_dir=user_dir,
                locked_fields=locked_fields,
            )

            # Step 3: Embed metadata into DOCX (using legacy accessors for standard fields)
            logger.debug("Embedding metadata into DOCX")
            docx_with_metadata = embed_metadata_in_docx(
                file_bytes,
                title=metadata.subject,
                author=metadata.sender,
                subject=f"{metadata.doc_type} - {metadata.topic}"
                if metadata.topic
                else metadata.doc_type,
                keywords=metadata.keywords,
            )

            # Step 4: Generate filename from metadata using context's pattern
            suggested_filename = metadata.to_filename(filename_pattern, source_filename=filename)
            if suggested_filename.endswith(".pdf"):
                suggested_filename = suggested_filename[:-4] + ".docx"

            logger.info(
                "DOCX processed successfully: %s -> %s (context: %s)",
                filename,
                suggested_filename,
                metadata.context,
            )

            # Build metadata response with all fields
            metadata_response = {
                "context": metadata.context,
                "date": metadata.date.isoformat() if metadata.date else None,
            }
            metadata_response.update(metadata.fields)
            
            response_data = {
                "filename": suggested_filename,
                "docx": base64.b64encode(docx_with_metadata).decode("utf-8"),
                "metadata": metadata_response,
            }

            # Include new clues if any were suggested
            if metadata.new_clues:
                response_data["new_clues"] = {
                    field: {"value": value, "clue": clue}
                    for field, (value, clue) in metadata.new_clues.items()
                }

            return web.json_response(response_data)

        except ConfigurationError as e:
            logger.error("Configuration error for DOCX %s: %s", filename, e)
            return web.json_response({"error": f"Configuration error: {e}"}, status=400)
        except AiError as e:
            logger.error("AI processing failed for DOCX %s: %s", filename, e)
            return web.json_response({"error": f"AI processing failed: {e}"}, status=502)
        except Exception as e:
            logger.exception("Internal error processing DOCX %s", filename)
            return web.json_response({"error": f"Internal error: {e}"}, status=500)

    async def _process_text(
        self,
        file_bytes: bytes,
        filename: str,
        primary_language: Optional[str],
        contexts: list[ContextConfig],
        user_dir: Optional[Path],
        locked_fields: Optional[dict[str, dict[str, Any]]] = None,
    ) -> web.Response:
        """
        Process a plain text file (.txt or .md).

        Text files are classified and renamed but not transformed.
        Returns metadata and suggested filename.
        """
        try:
            # Decode text content
            try:
                text = file_bytes.decode("utf-8")
            except UnicodeDecodeError:
                # Try latin-1 as fallback
                try:
                    text = file_bytes.decode("latin-1")
                except UnicodeDecodeError as e:
                    logger.error("Failed to decode text file %s: %s", filename, e)
                    return web.json_response(
                        {"error": f"Failed to decode text file: {e}"}, status=400
                    )

            if not text.strip():
                logger.warning("Text file %s is empty", filename)
                return web.json_response(
                    {"error": "Text file is empty"}, status=400
                )

            logger.info(
                "Processing text file: %s (%d chars), contexts=%d",
                filename,
                len(text),
                len(contexts),
            )

            # Extract metadata using AI
            logger.debug("Starting AI metadata extraction for text file")
            metadata, filename_pattern = await self.ai_client.extract_metadata(
                text,
                contexts=contexts,
                primary_language=primary_language,
                filename=filename,
                user_dir=user_dir,
                locked_fields=locked_fields,
            )

            # Generate filename from metadata, preserving original extension
            suggested_filename = metadata.to_filename(filename_pattern, source_filename=filename)
            # Replace .pdf extension with original extension
            original_ext = os.path.splitext(filename)[1].lower()
            if suggested_filename.endswith(".pdf"):
                suggested_filename = suggested_filename[:-4] + original_ext

            logger.info(
                "Text file processed successfully: %s -> %s (context: %s)",
                filename,
                suggested_filename,
                metadata.context,
            )

            # Build metadata response with all fields
            metadata_response = {
                "context": metadata.context,
                "date": metadata.date.isoformat() if metadata.date else None,
            }
            metadata_response.update(metadata.fields)

            response_data = {
                "filename": suggested_filename,
                "metadata": metadata_response,
            }

            # Include new clues if any were suggested
            if metadata.new_clues:
                response_data["new_clues"] = {
                    field: {"value": value, "clue": clue}
                    for field, (value, clue) in metadata.new_clues.items()
                }

            return web.json_response(response_data)

        except ConfigurationError as e:
            logger.error("Configuration error for text file %s: %s", filename, e)
            return web.json_response({"error": f"Configuration error: {e}"}, status=400)
        except AiError as e:
            logger.error("AI processing failed for text file %s: %s", filename, e)
            return web.json_response({"error": f"AI processing failed: {e}"}, status=502)
        except Exception as e:
            logger.exception("Internal error processing text file %s", filename)
            return web.json_response({"error": f"Internal error: {e}"}, status=500)

    async def process_transcript(self, request: web.Request) -> web.Response:
        """
        Process a transcript from STT service.

        Accepts JSON body with:
        - transcript: Full STT transcript object (with words)
        - contexts: Array of context configurations (required)
        - filename: Original filename for PDF title (optional)
        - primary_language: Output language for AI responses (optional)

        Process:
        1. Strip words from transcript JSON
        2. Create text from simplified JSON for classification
        3. Classify text to determine context (ignore new_clues)
        4. Build correction context from context description and chosen candidate clues
        5. Correct transcript using AI
        6. Generate PDF and text from corrected transcript

        Returns JSON with:
        - corrected_json: Corrected transcript (without words)
        - text: Plain text transcript
        - pdf_base64: Base64-encoded PDF
        - metadata: Extracted metadata fields including context
        """
        try:
            data = await request.json()
        except json.JSONDecodeError as e:
            logger.warning("Invalid JSON in request body: %s", e)
            return web.json_response({"error": "Invalid JSON in request body"}, status=400)

        transcript_json = data.get("transcript")
        filename = data.get("filename", "transcript")
        primary_language = data.get("primary_language")
        contexts = parse_contexts(data.get("contexts"))
        user_dir_str = data.get("user_dir")
        user_dir = Path(user_dir_str) if user_dir_str else None
        locked_fields: Optional[dict[str, dict[str, Any]]] = data.get("locked_fields")
        pre_classified: Optional[dict[str, Any]] = data.get("pre_classified")

        if transcript_json is None:
            logger.warning("No transcript provided in request")
            return web.json_response({"error": "transcript field is required"}, status=400)

        if not contexts:
            logger.warning("No contexts provided in request")
            return web.json_response(
                {"error": "contexts field is required with at least one context"},
                status=400,
            )

        logger.info(
            "Processing transcript: %s, segments=%d, contexts=%d, pre_classified=%s",
            filename,
            len(transcript_json.get("segments", [])),
            len(contexts),
            bool(pre_classified),
        )

        try:
            # Step 1: Strip words from transcript
            light_json = strip_words_from_json(transcript_json)

            if pre_classified:
                # Pre-classified flow: skip AI classification, use provided metadata
                pc_context = pre_classified.get("context")
                pc_date_str = pre_classified.get("date")
                pc_fields = pre_classified.get("fields", {})

                # Parse date
                pc_date = None
                if pc_date_str:
                    try:
                        pc_date = date.fromisoformat(pc_date_str)
                    except (ValueError, TypeError):
                        logger.warning("Invalid date in pre_classified: %s", pc_date_str)

                # Construct DocumentMetadata directly
                metadata = DocumentMetadata(
                    fields=dict(pc_fields),
                    date=pc_date,
                    context=pc_context,
                )

                # Look up filename_pattern from matching context config
                filename_pattern = None
                for ctx in contexts:
                    if ctx["name"] == pc_context:
                        filename_pattern = ctx.get("audio_filename") or ctx.get("filename")
                        break
                if not filename_pattern:
                    # Fallback: use first context's pattern
                    filename_pattern = contexts[0].get("filename", "{date}-{context}")

                logger.info(
                    "Using pre_classified metadata: context=%s, fields=%d",
                    pc_context, len(pc_fields),
                )
            else:
                # Standard flow: classify transcript via AI
                # Step 2: Create text for classification
                classification_text = create_text_content(light_json)
                logger.debug("Created classification text: %d chars", len(classification_text))

                # Prepend transcript-specific instruction for metadata extraction
                transcript_instruction = (
                    "Note: This is a transcript of an audio recording. "
                    "With high probability, metadata such as document type, date, subject, "
                    "or participants has been dictated at the beginning or end of the recording. "
                    "Pay special attention to the first and last segments for this information.\n\n"
                )
                classification_text = transcript_instruction + classification_text

                # Step 3: Classify text to determine context (ignore new_clues)
                metadata, filename_pattern = await self.ai_client.extract_metadata(
                    classification_text,
                    contexts=contexts,
                    primary_language=primary_language,
                    filename=filename,
                    user_dir=user_dir,
                    locked_fields=locked_fields,
                    is_audio=True,
                )
                # Explicitly ignore new_clues for transcripts
                metadata.new_clues = {}

            # Step 4: Build correction context from classification
            correction_context = self._build_correction_context(metadata, contexts)
            logger.debug("Built correction context: %d chars", len(correction_context))

            # Step 5: Correct transcript (run in thread pool to avoid blocking)
            logger.info("Correcting transcript with context...")
            corrected_json = await asyncio.to_thread(
                correct_transcript_json,
                light_json,
                model=self.correction_model,
                extended_thinking=self.correction_extended_thinking,
                thinking_budget=self.correction_thinking_budget,
                context=correction_context,
                use_batch=self.correction_use_batch,
                user_dir=user_dir,
            )

            # Step 6: Generate text from corrected transcript
            text_content = create_text_content(corrected_json)

            # Step 7: Generate suggested filename from metadata using context's pattern
            suggested_filename = metadata.to_filename(filename_pattern, source_filename=filename)

            logger.info(
                "Transcript processed successfully: %s -> %s (context: %s)",
                filename,
                suggested_filename,
                metadata.context,
            )

            # Build metadata response with all fields
            metadata_response = {
                "context": metadata.context,
                "date": metadata.date.isoformat() if metadata.date else None,
            }
            metadata_response.update(metadata.fields)

            return web.json_response({
                "corrected_json": corrected_json,
                "text": text_content,
                "filename": suggested_filename,
                "metadata": metadata_response,
            })

        except ConfigurationError as e:
            logger.error("Configuration error for transcript %s: %s", filename, e)
            return web.json_response({"error": f"Configuration error: {e}"}, status=400)
        except AiError as e:
            logger.error("AI processing failed for transcript %s: %s", filename, e)
            return web.json_response({"error": f"AI processing failed: {e}"}, status=502)
        except Exception as e:
            logger.exception("Internal error processing transcript %s", filename)
            return web.json_response({"error": f"Internal error: {e}"}, status=500)

    async def classify_audio(self, request: web.Request) -> web.Response:
        """
        Classify an audio file based on filename only.

        Used by syncthing watcher to determine context and gather transcription_keyterms
        BEFORE calling the STT service.

        Accepts JSON body with:
        - filename: Original audio filename (required)
        - contexts: Array of context configurations (required)

        Returns JSON with:
        - context: Determined context name (or null if undetermined)
        - metadata: Extracted metadata fields
        - transcription_keyterms: Combined list of keyterms from context and matched candidates
        """
        try:
            data = await request.json()
        except json.JSONDecodeError as e:
            logger.warning("Invalid JSON in request body: %s", e)
            return web.json_response({"error": "Invalid JSON in request body"}, status=400)

        filename = data.get("filename")
        contexts = parse_contexts(data.get("contexts"))

        if not filename:
            logger.warning("No filename provided in request")
            return web.json_response({"error": "filename field is required"}, status=400)

        if not contexts:
            logger.warning("No contexts provided in request")
            return web.json_response(
                {"error": "contexts field is required with at least one context"},
                status=400,
            )

        logger.info("Classifying audio file: %s, contexts=%d", filename, len(contexts))

        try:
            # Use filename as the "text" for classification
            # The AI will classify based on filename patterns
            classification_text = f"Audio file: {filename}"

            # Determine context
            metadata, filename_pattern = await self.ai_client.extract_metadata(
                classification_text,
                contexts=contexts,
                primary_language=None,
                filename=filename,
            )

            # Collect transcription_keyterms
            keyterms_list = self._collect_transcription_keyterms(metadata, contexts)

            # Build metadata response
            metadata_response = {
                "context": metadata.context,
                "date": metadata.date.isoformat() if metadata.date else None,
            }
            metadata_response.update(metadata.fields)

            logger.info(
                "Audio classified: context=%s, keyterms=%d",
                metadata.context, len(keyterms_list)
            )
            logger.debug(
                "[classify_audio] Result: context=%s, fields=%s, keyterms=%s",
                metadata.context, metadata.fields, keyterms_list
            )

            return web.json_response({
                "context": metadata.context,
                "metadata": metadata_response,
                "transcription_keyterms": keyterms_list,
            })

        except ConfigurationError as e:
            logger.error("Configuration error classifying %s: %s", filename, e)
            return web.json_response({"error": f"Configuration error: {e}"}, status=400)
        except AiError as e:
            logger.error("AI classification failed for %s: %s", filename, e)
            return web.json_response({"error": f"AI classification failed: {e}"}, status=502)
        except Exception as e:
            logger.exception("Internal error classifying %s", filename)
            return web.json_response({"error": f"Internal error: {e}"}, status=500)

    async def classify_transcript(self, request: web.Request) -> web.Response:
        """
        Classify a transcript from an intro audio file.

        Used for the two-pass STT flow: extracts richer metadata (including
        number_of_speakers) from the first-pass transcript, providing better
        keyterms and speaker count for the second STT pass.

        Accepts JSON body with:
        - transcript: Full STT transcript object (with words)
        - filename: Original audio filename (required)
        - contexts: Array of context configurations (required)

        Returns JSON with:
        - context: Determined context name
        - metadata: Extracted metadata fields (without number_of_speakers)
        - transcription_keyterms: Combined keyterms for second STT pass
        - number_of_speakers: Extracted speaker count (or null)
        """
        try:
            data = await request.json()
        except json.JSONDecodeError as e:
            logger.warning("Invalid JSON in request body: %s", e)
            return web.json_response({"error": "Invalid JSON in request body"}, status=400)

        transcript_json = data.get("transcript")
        filename = data.get("filename")
        contexts = parse_contexts(data.get("contexts"))

        if transcript_json is None:
            logger.warning("No transcript provided in request")
            return web.json_response({"error": "transcript field is required"}, status=400)

        if not filename:
            logger.warning("No filename provided in request")
            return web.json_response({"error": "filename field is required"}, status=400)

        if not contexts:
            logger.warning("No contexts provided in request")
            return web.json_response(
                {"error": "contexts field is required with at least one context"},
                status=400,
            )

        logger.info("Classifying transcript for intro file: %s, contexts=%d", filename, len(contexts))

        try:
            # Step 1: Strip words and create text from transcript
            light_json = strip_words_from_json(transcript_json)
            classification_text = create_text_content(light_json)

            # Step 2: Prepend intro-specific instruction
            intro_instruction = (
                "At the beginning or the end of the transcript you will find meta information "
                "spoken by the person who created the recording. This may include: Date, speaker, "
                "number of speakers, background, disturbances in the recording, interruptions, "
                "number of related files, possible core interpretation, important open questions, etc.\n\n"
            )
            classification_text = intro_instruction + classification_text

            # Step 3: Determine context
            if len(contexts) == 1:
                context_name = contexts[0]["name"]
            else:
                context_name = await self.ai_client.determine_context(
                    classification_text, contexts, filename=filename,
                    include_all_candidates=True,
                )

            # Find the matching context config
            context_config: Optional[ContextConfig] = None
            for ctx in contexts:
                if ctx["name"] == context_name:
                    context_config = ctx
                    break

            if context_config is None:
                return web.json_response(
                    {"error": f"Context '{context_name}' not found"}, status=400
                )

            # Step 4: Deep-copy field configs and inject number_of_speakers
            field_configs = copy.deepcopy(context_config.get("fields", {}))
            field_configs["number_of_speakers"] = {
                "instructions": (
                    "The number of speakers/participants mentioned in the recording intro. "
                    "Return as an integer string, e.g. '2'."
                ),
            }

            # Step 5: Extract metadata with augmented fields
            metadata = await self.ai_client._extract_metadata_with_config(
                text=classification_text,
                field_configs=field_configs,
                context_name=context_name,
                filename=filename,
            )
            metadata.new_clues = {}

            # Step 6: Extract and remove number_of_speakers from metadata
            number_of_speakers = metadata.fields.pop("number_of_speakers", None)

            # Try to parse as integer
            if number_of_speakers is not None:
                try:
                    number_of_speakers = int(number_of_speakers)
                except (ValueError, TypeError):
                    logger.warning(
                        "Could not parse number_of_speakers '%s' as int",
                        number_of_speakers,
                    )
                    number_of_speakers = None

            # Step 7: Collect keyterms
            keyterms_list = self._collect_transcription_keyterms(metadata, contexts)

            # Build metadata response
            metadata_response = {
                "context": metadata.context,
                "date": metadata.date.isoformat() if metadata.date else None,
            }
            metadata_response.update(metadata.fields)

            logger.info(
                "Transcript classified: context=%s, keyterms=%d, speakers=%s",
                metadata.context, len(keyterms_list), number_of_speakers,
            )

            return web.json_response({
                "context": metadata.context,
                "metadata": metadata_response,
                "transcription_keyterms": keyterms_list,
                "number_of_speakers": number_of_speakers,
            })

        except ConfigurationError as e:
            logger.error("Configuration error classifying transcript %s: %s", filename, e)
            return web.json_response({"error": f"Configuration error: {e}"}, status=400)
        except AiError as e:
            logger.error("AI classification failed for transcript %s: %s", filename, e)
            return web.json_response({"error": f"AI classification failed: {e}"}, status=502)
        except Exception as e:
            logger.exception("Internal error classifying transcript %s", filename)
            return web.json_response({"error": f"Internal error: {e}"}, status=500)

    def _collect_transcription_keyterms(
        self,
        metadata,
        contexts: list[ContextConfig],
    ) -> list[str]:
        """
        Collect transcription keyterms from classification results.

        Gathers keyterms from:
        - Context-level transcription_keyterms
        - Matched candidate transcription_keyterms for each field

        Returns sorted deduplicated list of keyterms.
        """
        keyterms: set[str] = set()

        # Find the context configuration
        context_config: Optional[ContextConfig] = None
        for ctx in contexts:
            if ctx["name"] == metadata.context:
                context_config = ctx
                break

        if context_config:
            # Add context-level keyterms
            context_keyterms = context_config.get("transcription_keyterms", [])
            if context_keyterms:
                keyterms.update(context_keyterms)
                logger.debug(
                    "Context '%s' keyterms: %s",
                    metadata.context, context_keyterms
                )

            # Add keyterms from matched candidates
            fields_config = context_config.get("fields", {})
            for field_name, field_config in fields_config.items():
                value = metadata.fields.get(field_name)
                if not value:
                    continue

                candidates = field_config.get("candidates", [])
                if not candidates:
                    continue

                for candidate in candidates:
                    if isinstance(candidate, dict):
                        candidate_name = candidate.get("name")
                        candidate_short = candidate.get("short")
                        if value == candidate_name or value == candidate_short:
                            candidate_keyterms = candidate.get("transcription_keyterms", [])
                            if candidate_keyterms:
                                keyterms.update(candidate_keyterms)
                                logger.debug(
                                    "Field '%s' candidate '%s' keyterms: %s",
                                    field_name, value, candidate_keyterms
                                )
                            break

        return sorted(keyterms) if keyterms else []

    def _build_correction_context(
        self,
        metadata,
        contexts: list[ContextConfig],
    ) -> str:
        """
        Build correction context from classification results.

        Includes:
        - Context description
        - Clues from chosen candidates for all fields with candidates
        """
        parts = []

        # Find the context configuration
        context_config: Optional[ContextConfig] = None
        for ctx in contexts:
            if ctx["name"] == metadata.context:
                context_config = ctx
                break

        if context_config:
            # Add context description
            parts.append(f"Document context: {context_config['description']}")

            # Add clues from chosen candidates for all fields
            fields_config = context_config.get("fields", {})
            for field_name, field_config in fields_config.items():
                value = metadata.fields.get(field_name)
                if not value:
                    continue

                candidates = field_config.get("candidates", [])
                if not candidates:
                    continue
                    
                for candidate in candidates:
                    if isinstance(candidate, dict):
                        if candidate.get("name") == value:
                            clues = candidate.get("clues", [])
                            if clues:
                                clues_text = "; ".join(clues)
                                parts.append(f"{field_name.capitalize()} '{value}': {clues_text}")
                            break

        return "\n".join(parts)

    def create_app(self) -> web.Application:
        """Create the aiohttp application."""
        app = web.Application(client_max_size=100 * 1024 * 1024)  # 100MB max
        app.router.add_get("/health", self.health)
        app.router.add_post("/process", self.process_document)
        app.router.add_post("/process_transcript", self.process_transcript)
        app.router.add_post("/classify_audio", self.classify_audio)
        app.router.add_post("/classify_transcript", self.classify_transcript)

        # Initialize cost tracker on startup
        async def on_startup(app):
            get_cost_tracker()  # Ensures tracker is started

        # Shutdown cost tracker on cleanup
        async def on_cleanup(app):
            shutdown_cost_tracker()

        app.on_startup.append(on_startup)
        app.on_cleanup.append(on_cleanup)

        return app


def main():
    """Entry point for the server."""
    # Configure logging
    log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
    log_dir = os.environ.get("LOG_DIR")

    handlers = [logging.StreamHandler()]
    if log_dir:
        Path(log_dir).mkdir(parents=True, exist_ok=True)
        log_file = Path(log_dir) / "mrdocument.log"
        handlers.append(logging.FileHandler(log_file))

    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=handlers,
    )

    ocr_url = os.environ.get("OCR_URL", "http://ocrmypdf:5000")
    anthropic_api_key = os.environ.get("ANTHROPIC_API_KEY")
    # Only pass model if explicitly set in environment (otherwise uses config.yaml)
    anthropic_model = os.environ.get("ANTHROPIC_MODEL")
    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", "8000"))

    if not anthropic_api_key:
        logger.error("ANTHROPIC_API_KEY environment variable is required")
        exit(1)

    logger.info("Starting MrDocument server on %s:%d", host, port)
    server = MrDocumentServer(ocr_url, anthropic_api_key, anthropic_model)
    app = server.create_app()
    web.run_app(app, host=host, port=port)


if __name__ == "__main__":
    main()
