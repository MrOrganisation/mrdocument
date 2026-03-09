"""CLI interface for STT tool."""

import json
import os
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn

from .config import Config, set_config_path, get_config_path
from .convert import needs_conversion, is_supported, convert_to_flac
from .transcript import TranscriptResult
from .postprocess import correct_json_light, strip_words_from_json, transcript_to_json
from .output import write_pdf_from_json, write_text_from_json
from .backends.elevenlabs import ElevenLabsBackend


def version_callback(value: bool) -> None:
    if value:
        console.print("stt-cli version 0.1.0")
        raise typer.Exit()


def config_callback(ctx: typer.Context, config_path: Optional[Path]) -> None:
    """Process global --config option."""
    if config_path:
        set_config_path(config_path)


app = typer.Typer(
    help="STT - Speech-to-Text CLI tool (ElevenLabs + Anthropic)",
    callback=lambda config: None,  # Placeholder, real callback below
)
config_app = typer.Typer(help="Configuration commands")
app.add_typer(config_app, name="config")

console = Console()


@app.callback()
def main_callback(
    config: Optional[Path] = typer.Option(
        None,
        "--config",
        "-c",
        help="Path to config file (default: ~/.config/stt-cli/config.yaml)",
        exists=False,  # Allow non-existent file (will be created on save)
    ),
) -> None:
    """STT - Speech-to-Text CLI tool."""
    if config:
        set_config_path(config)


@config_app.command("show")
def config_show() -> None:
    """Show current configuration."""
    config_path = get_config_path()
    cfg = Config.load()
    
    console.print("[bold]Current Configuration:[/bold]")
    console.print(f"  Config file: {config_path}")
    if config_path.exists():
        console.print("  [dim](file exists)[/dim]")
    else:
        console.print("  [dim](using defaults, file not yet created)[/dim]")
    console.print("")
    console.print("[bold]  ElevenLabs Settings:[/bold]")
    console.print(f"    Model: {cfg.elevenlabs_model}")
    console.print(f"    ELEVENLABS_API_KEY: {'[set]' if os.environ.get('ELEVENLABS_API_KEY') else '[not set]'}")
    console.print("")
    console.print("[bold]  Anthropic Settings (post-processing):[/bold]")
    console.print(f"    Model: {cfg.anthropic_model}")
    console.print(f"    ANTHROPIC_API_KEY: {'[set]' if os.environ.get('ANTHROPIC_API_KEY') else '[not set]'}")
    console.print(f"    Extended Thinking: {cfg.extended_thinking}")
    console.print(f"    Thinking Budget: {cfg.thinking_budget}")
    console.print(f"    Batch API: {cfg.use_batch_api}")
    if cfg.correction_context:
        # Truncate long context for display
        ctx_display = cfg.correction_context[:100] + "..." if len(cfg.correction_context) > 100 else cfg.correction_context
        console.print(f"    Correction Context: {ctx_display}")
    else:
        console.print("    Correction Context: [dim](not set)[/dim]")
    console.print("")
    console.print("[bold]  Common Settings:[/bold]")
    console.print(f"    Default Language: {cfg.default_language}")
    console.print(f"    Diarization: {cfg.enable_diarization}")
    console.print(f"    Word Timestamps: {cfg.enable_word_timestamps}")
    console.print(f"    Speaker Count: {cfg.diarization_speaker_count}")


@config_app.command("set")
def config_set(
    model: Optional[str] = typer.Option(None, "--model", "-m", help="ElevenLabs model (scribe_v1, scribe_v1_experimental, scribe_v2)"),
    anthropic_model: Optional[str] = typer.Option(None, "--anthropic-model", help="Anthropic model for correction"),
    language: Optional[str] = typer.Option(None, "--language", "-l", help="Default language code"),
    diarization: Optional[bool] = typer.Option(None, "--diarization", "-d", help="Enable diarization"),
    timestamps: Optional[bool] = typer.Option(None, "--timestamps", "-t", help="Enable word timestamps"),
    speakers: Optional[int] = typer.Option(None, "--speakers", "-s", help="Expected speaker count"),
) -> None:
    """Set configuration values."""
    cfg = Config.load()

    if model is not None:
        cfg.elevenlabs_model = model
    if anthropic_model is not None:
        cfg.anthropic_model = anthropic_model
    if language is not None:
        cfg.default_language = language
    if diarization is not None:
        cfg.enable_diarization = diarization
    if timestamps is not None:
        cfg.enable_word_timestamps = timestamps
    if speakers is not None:
        cfg.diarization_speaker_count = speakers

    cfg.save()
    console.print("[green]Configuration saved.[/green]")


@app.command("transcribe")
def transcribe(
    file: Path = typer.Argument(..., help="Path to the audio file"),
    language: Optional[str] = typer.Option(None, "--language", "-l", help="Language code (e.g., de, en, de-DE)"),
    model: Optional[str] = typer.Option(None, "--model", "-m", help="ElevenLabs model (scribe_v1, scribe_v1_experimental, scribe_v2)"),
    keyterms: Optional[str] = typer.Option(None, "--keyterms", "-k", help="Comma-separated key terms to help transcription accuracy"),
    no_correct: bool = typer.Option(False, "--no-correct", help="Skip Anthropic correction"),
    thinking: Optional[bool] = typer.Option(None, "--thinking", help="Enable extended thinking for correction"),
    thinking_budget: Optional[int] = typer.Option(None, "--thinking-budget", help="Token budget for extended thinking"),
    no_batch: bool = typer.Option(False, "--no-batch", help="Disable batch API (use direct request)"),
    diarization: Optional[bool] = typer.Option(None, "--diarization", "-d", help="Enable speaker diarization"),
    timestamps: Optional[bool] = typer.Option(None, "--timestamps", "-t", help="Enable word timestamps"),
    speakers: Optional[int] = typer.Option(None, "--speakers", "-s", help="Expected number of speakers"),
    debug: bool = typer.Option(False, "--debug", help="Enable debug output"),
) -> None:
    """
    Transcribe an audio file using ElevenLabs with Anthropic correction.
    
    Output files:
    - <input>_full.json: Original transcript with word-level data
    - <input>_raw.json: Original transcript without words (sent to Anthropic)
    - <input>.json: Corrected transcript (from Anthropic)
    - <input>.pdf: PDF document from corrected transcript
    - <input>.txt: Plain text from corrected transcript
    """
    cfg = Config.load()

    if not file.exists():
        console.print(f"[red]Error: File not found: {file}[/red]")
        raise typer.Exit(1)

    if not is_supported(file) and not needs_conversion(file):
        console.print(f"[red]Error: Unsupported file format: {file.suffix}[/red]")
        console.print("Supported formats: .flac, .wav, .mp3, .ogg, .webm, .mp4, .m4a, .mkv, .avi, .mov")
        raise typer.Exit(1)

    if not os.environ.get("ELEVENLABS_API_KEY"):
        console.print("[red]Error: ELEVENLABS_API_KEY environment variable not set.[/red]")
        console.print("[yellow]Get your API key from: https://elevenlabs.io/app/settings/api-keys[/yellow]")
        raise typer.Exit(1)

    if not no_correct and not os.environ.get("ANTHROPIC_API_KEY"):
        console.print("[red]Error: ANTHROPIC_API_KEY environment variable not set.[/red]")
        console.print("[yellow]Get your API key from: https://console.anthropic.com/[/yellow]")
        console.print("[yellow]Or use --no-correct to skip correction.[/yellow]")
        raise typer.Exit(1)

    # Output files
    base_path = file.parent / file.stem
    full_json_path = Path(str(base_path) + "_full.json")
    raw_json_path = Path(str(base_path) + "_raw.json")
    corrected_json_path = base_path.with_suffix(".json")
    pdf_path = base_path.with_suffix(".pdf")
    txt_path = base_path.with_suffix(".txt")

    # Use config defaults if options not provided
    use_model = model or cfg.elevenlabs_model
    lang = language or cfg.default_language
    enable_diarization = diarization if diarization is not None else cfg.enable_diarization
    enable_timestamps = timestamps if timestamps is not None else cfg.enable_word_timestamps
    speaker_count = speakers or cfg.diarization_speaker_count
    use_thinking = thinking if thinking is not None else cfg.extended_thinking
    use_thinking_budget = thinking_budget if thinking_budget is not None else cfg.thinking_budget
    use_batch = cfg.use_batch_api and not no_batch  # Config default, overridden by --no-batch

    process_file = file
    converted_file: Optional[Path] = None

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        # Convert if necessary
        if needs_conversion(file):
            progress.add_task(f"Converting {file.suffix} to FLAC...", total=None)
            try:
                converted_file = convert_to_flac(file)
                process_file = converted_file
                console.print(f"Converted to {converted_file}")
            except (FileNotFoundError, RuntimeError) as e:
                console.print(f"[red]Error: {e}[/red]")
                raise typer.Exit(1)

        # Step 1: ElevenLabs transcription
        progress.add_task(f"Transcribing with ElevenLabs ({use_model})...", total=None)

        # Parse keyterms if provided
        keyterms_list: Optional[list[str]] = None
        if keyterms:
            keyterms_list = [k.strip() for k in keyterms.split(",") if k.strip()]

        try:
            backend = ElevenLabsBackend(model=use_model)
            job = backend.transcribe(
                audio_path=process_file,
                language=lang,
                enable_diarization=enable_diarization,
                speaker_count=speaker_count,
                enable_word_timestamps=enable_timestamps,
                keyterms=keyterms_list,
            )
        except ValueError as e:
            console.print(f"[red]Error: {e}[/red]")
            raise typer.Exit(1)
        except RuntimeError as e:
            console.print(f"[red]Transcription failed: {e}[/red]")
            if debug:
                import traceback
                console.print(traceback.format_exc())
            raise typer.Exit(1)
        except Exception as e:
            console.print(f"[red]Transcription failed: {e}[/red]")
            if debug:
                import traceback
                console.print(traceback.format_exc())
            raise typer.Exit(1)

    # Clean up converted file
    if converted_file and converted_file.exists():
        converted_file.unlink()

    transcript = job.result
    if not transcript or not transcript.segments:
        console.print("[yellow]Warning: No transcript segments returned.[/yellow]")
        raise typer.Exit(1)

    console.print(f"\n[green]Transcription complete![/green]")
    console.print(f"[dim]Language: {transcript.language_code}[/dim]")
    console.print(f"[dim]Segments: {len(transcript.segments)}[/dim]")

    # Check if we got speaker labels
    has_speakers = any(seg.speaker_tag is not None for seg in transcript.segments)
    if has_speakers:
        unique_speakers = set(seg.speaker_tag for seg in transcript.segments if seg.speaker_tag is not None)
        console.print(f"[dim]Speakers: {len(unique_speakers)}[/dim]")

    # Aggregate by speaker if diarization was used
    if has_speakers and transcript.get_all_words():
        transcript = transcript.aggregate_by_speaker()
        if debug:
            console.print(f"[dim]Aggregated to {len(transcript.segments)} speaker segments[/dim]")

    # Convert to full JSON (with words)
    full_json = transcript_to_json(transcript, include_words=True)
    
    # Save full JSON
    full_json_path.write_text(json.dumps(full_json, indent=2, ensure_ascii=False))
    console.print(f"[green]Full JSON saved to:[/green] {full_json_path}")

    # Create light JSON (without words)
    light_json = strip_words_from_json(full_json)
    
    # Save raw (uncorrected) light JSON
    raw_json_path.write_text(json.dumps(light_json, indent=2, ensure_ascii=False))
    console.print(f"[green]Raw JSON saved to:[/green] {raw_json_path}")

    # Step 2: Anthropic correction (unless disabled)
    if not no_correct:
        mode_info = f"model={cfg.anthropic_model}"
        if use_thinking:
            mode_info += f", thinking={use_thinking_budget}"
        console.print(f"\n[cyan]Correcting with Anthropic ({mode_info})...[/cyan]")

        try:
            corrected_json = correct_json_light(
                light_json,
                model=cfg.anthropic_model,
                extended_thinking=use_thinking,
                thinking_budget=use_thinking_budget,
                use_batch=use_batch,
                context=cfg.correction_context,
            )
            console.print(f"[green]Correction complete![/green]")
            
            # Save corrected JSON
            corrected_json_path.write_text(json.dumps(corrected_json, indent=2, ensure_ascii=False))
            console.print(f"[green]Corrected JSON saved to:[/green] {corrected_json_path}")
            
            # Generate PDF from corrected transcript
            write_pdf_from_json(corrected_json, pdf_path, title=file.stem)
            console.print(f"[green]PDF saved to:[/green] {pdf_path}")
            
            # Generate text file from corrected transcript
            write_text_from_json(corrected_json, txt_path)
            console.print(f"[green]Text saved to:[/green] {txt_path}")
            
        except ValueError as e:
            console.print(f"[red]Error: {e}[/red]")
            raise typer.Exit(1)
        except RuntimeError as e:
            console.print(f"[red]Correction failed: {e}[/red]")
            if debug:
                import traceback
                console.print(traceback.format_exc())
            raise typer.Exit(1)


@app.command("correct")
def correct(
    json_file: Path = typer.Argument(..., help="Path to the JSON file to correct (e.g., <input>_raw.json)"),
    output: Optional[Path] = typer.Option(None, "--output", "-o", help="Output file (default: <input>_corrected.json)"),
    thinking: Optional[bool] = typer.Option(None, "--thinking", help="Enable extended thinking"),
    thinking_budget: Optional[int] = typer.Option(None, "--thinking-budget", help="Token budget for extended thinking"),
    no_batch: bool = typer.Option(False, "--no-batch", help="Disable batch API"),
    debug: bool = typer.Option(False, "--debug", help="Enable debug output"),
) -> None:
    """
    Correct an existing JSON transcript using Anthropic.
    
    Use this to retry correction on a _raw.json file without re-transcribing.
    """
    cfg = Config.load()

    if not json_file.exists():
        console.print(f"[red]Error: File not found: {json_file}[/red]")
        raise typer.Exit(1)

    if not os.environ.get("ANTHROPIC_API_KEY"):
        console.print("[red]Error: ANTHROPIC_API_KEY environment variable not set.[/red]")
        console.print("[yellow]Get your API key from: https://console.anthropic.com/[/yellow]")
        raise typer.Exit(1)

    # Use config defaults if options not provided
    use_thinking = thinking if thinking is not None else cfg.extended_thinking
    use_thinking_budget = thinking_budget if thinking_budget is not None else cfg.thinking_budget
    use_batch = cfg.use_batch_api and not no_batch  # Config default, overridden by --no-batch

    # Determine output path
    if output:
        output_path = output
    else:
        # Default: replace _raw with _corrected, or add _corrected
        stem = json_file.stem
        if stem.endswith("_raw"):
            output_path = json_file.parent / f"{stem[:-4]}.json"
        else:
            output_path = json_file.parent / f"{stem}_corrected.json"

    # Load JSON
    try:
        with open(json_file) as f:
            transcript_json = json.load(f)
    except json.JSONDecodeError as e:
        console.print(f"[red]Error: Invalid JSON file: {e}[/red]")
        raise typer.Exit(1)

    segments = transcript_json.get("segments", [])
    console.print(f"[dim]Loaded {len(segments)} segments from {json_file}[/dim]")

    # Run correction
    mode_info = f"model={cfg.anthropic_model}"
    if use_thinking:
        mode_info += f", thinking={use_thinking_budget}"
    if not use_batch:
        mode_info += ", sync"
    console.print(f"\n[cyan]Correcting with Anthropic ({mode_info})...[/cyan]")

    try:
        corrected_json = correct_json_light(
            transcript_json,
            model=cfg.anthropic_model,
            extended_thinking=use_thinking,
            thinking_budget=use_thinking_budget,
            use_batch=use_batch,
            context=cfg.correction_context,
        )
        console.print(f"[green]Correction complete![/green]")
        
        # Save corrected JSON
        output_path.write_text(json.dumps(corrected_json, indent=2, ensure_ascii=False))
        console.print(f"[green]Corrected JSON saved to:[/green] {output_path}")
        
    except ValueError as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1)
    except RuntimeError as e:
        console.print(f"[red]Correction failed: {e}[/red]")
        if debug:
            import traceback
            console.print(traceback.format_exc())
        raise typer.Exit(1)


@app.command("serve")
def serve(
    host: str = typer.Option("0.0.0.0", "--host", "-h", help="Host to bind to"),
    port: int = typer.Option(8000, "--port", "-p", help="Port to bind to"),
) -> None:
    """
    Start the HTTP API server.
    
    The API provides endpoints for transcription:
    - POST /transcribe - Returns JSON with full_json, corrected_json, and pdf_base64
    - POST /transcribe/pdf - Returns PDF file directly
    - GET /health - Health check
    """
    import uvicorn
    from .api import app as api_app
    
    console.print(f"[cyan]Starting STT API server on {host}:{port}...[/cyan]")
    console.print(f"[dim]API docs available at http://{host}:{port}/docs[/dim]")
    
    if not os.environ.get("ELEVENLABS_API_KEY"):
        console.print("[yellow]Warning: ELEVENLABS_API_KEY not set[/yellow]")
    if not os.environ.get("ANTHROPIC_API_KEY"):
        console.print("[yellow]Warning: ANTHROPIC_API_KEY not set[/yellow]")
    
    uvicorn.run(api_app, host=host, port=port)


def main() -> None:
    """Entry point for the CLI."""
    app()


if __name__ == "__main__":
    main()
