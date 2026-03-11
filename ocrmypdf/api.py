#!/usr/bin/env python3
"""
Simple Flask API for OCRmyPDF service
"""
import logging
import os
import subprocess
import tempfile
import base64
from pathlib import Path
from flask import Flask, request, send_file, jsonify
from werkzeug.utils import secure_filename

# Configure logging
log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
log_dir = os.environ.get("LOG_DIR")

handlers = [logging.StreamHandler()]
if log_dir:
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    log_file = Path(log_dir) / "ocrmypdf.log"
    handlers.append(logging.FileHandler(log_file))

logging.basicConfig(
    level=getattr(logging, log_level, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=handlers,
)
logger = logging.getLogger(__name__)

def _read_version() -> str:
    for p in (Path(__file__).parent / "VERSION", Path("/app/VERSION")):
        if p.is_file():
            return p.read_text().strip()
    return "unknown"

APP_VERSION = _read_version()

app = Flask(__name__)

logger.info("OCRmyPDF service starting (version %s)", APP_VERSION)

# Configure upload settings
UPLOAD_FOLDER = '/tmp/uploads'
OUTPUT_FOLDER = '/tmp/outputs'
MAX_CONTENT_LENGTH = 100 * 1024 * 1024  # 100MB max file size

Path(UPLOAD_FOLDER).mkdir(exist_ok=True)
Path(OUTPUT_FOLDER).mkdir(exist_ok=True)

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['OUTPUT_FOLDER'] = OUTPUT_FOLDER
app.config['MAX_CONTENT_LENGTH'] = MAX_CONTENT_LENGTH


@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({'status': 'healthy', 'service': 'ocrmypdf'})


@app.route('/ocr', methods=['POST'])
def ocr_pdf():
    """
    OCR a PDF file
    
    Parameters (form-data):
    - file: PDF file to process
    - language: OCR language (default: eng, can be comma or plus-separated like 'eng,deu' or 'eng+deu')
    - skip_text: Skip pages that already have text (default: false)
    - force_ocr: Force OCR on all pages, even if they already have text (default: true)
    - optimize: Optimize PDF size (0-3, default: 1)
    - deskew: Deskew crooked scans (default: false)
    - clean: Clean pages before OCR (default: false)
    - return_text: Return extracted text along with PDF as JSON (default: false)
    - text_only: Return only extracted text as JSON without PDF (default: false)
    """
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400
    
    if not file.filename.lower().endswith('.pdf'):
        return jsonify({'error': 'Only PDF files are supported'}), 400
    
    # Get options from form data
    language = request.form.get('language', 'eng')
    # Convert comma-separated to plus-separated for ocrmypdf
    language = language.replace(',', '+')
    skip_text = request.form.get('skip_text', 'false').lower() == 'true'
    force_ocr = request.form.get('force_ocr', 'false').lower() == 'true'
    optimize = request.form.get('optimize', '1')
    deskew = request.form.get('deskew', 'false').lower() == 'true'
    clean = request.form.get('clean', 'false').lower() == 'true'
    return_text = request.form.get('return_text', 'false').lower() == 'true'
    text_only = request.form.get('text_only', 'false').lower() == 'true'
    
    try:
        # Save uploaded file - truncate filename to avoid filesystem limits
        filename = secure_filename(file.filename)
        # Limit filename to 100 chars (plus extension) to stay well under 255 limit
        if len(filename) > 104:
            name, ext = os.path.splitext(filename)
            filename = name[:100] + ext
        input_path = os.path.join(UPLOAD_FOLDER, filename)
        output_path = os.path.join(OUTPUT_FOLDER, f'ocr_{filename}')
        
        file.save(input_path)
        
        # Build ocrmypdf command
        # IMPORTANT: --invalidate-digital-signatures MUST be specified to allow OCR of signed PDFs
        cmd = ['ocrmypdf', '--invalidate-digital-signatures']
        
        # Add language
        cmd.extend(['-l', language])
        
        # Add other options
        cmd.extend(['--optimize', optimize, '--output-type', 'pdf'])
        
        if skip_text:
            cmd.append('--skip-text')
        if force_ocr:
            cmd.append('--force-ocr')
        if deskew:
            cmd.append('--deskew')
        if clean:
            cmd.append('--clean')
        
        cmd.extend([input_path, output_path])
        
        # Log the exact command being run
        cmd_str = ' '.join(cmd)
        app.logger.info(f"Running OCR command: {cmd_str}")
        
        # Run OCR
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=300  # 5 minute timeout
        )
        
        # Log the result
        app.logger.info(f"OCR return code: {result.returncode}")
        if result.stderr:
            app.logger.info(f"OCR stderr (first 500 chars): {result.stderr[:500]}")
        
        # Clean up input file
        os.remove(input_path)
        
        # Check if output was created (even if there were warnings)
        output_exists = os.path.exists(output_path) and os.path.getsize(output_path) > 0
        
        # Check if digital signatures were invalidated (look in stderr)
        signature_invalidated = (
            'digital signature' in result.stderr.lower() or 
            'digitally signed' in result.stderr.lower() or
            'invalidating the signature' in result.stderr.lower()
        )
        
        # OCRmyPDF may return non-zero for warnings but still produce valid output
        # Treat as success if output file exists and has content
        if result.returncode == 0 or output_exists:
            
            # Check if text extraction is requested
            if text_only or return_text:
                # Extract text from the OCR'd PDF using pdftotext
                text_result = subprocess.run(
                    ['pdftotext', output_path, '-'],
                    capture_output=True,
                    text=True
                )
                
                if text_result.returncode != 0:
                    os.remove(output_path)
                    return jsonify({
                        'error': 'Text extraction failed',
                        'details': text_result.stderr
                    }), 500
                
                extracted_text = text_result.stdout
                
                if text_only:
                    # Return only the extracted text
                    os.remove(output_path)
                    response_data = {
                        'text': extracted_text,
                        'filename': filename
                    }
                    if signature_invalidated:
                        response_data['signature_invalidated'] = True
                    return jsonify(response_data)
                else:
                    # Return both PDF (as base64) and text
                    with open(output_path, 'rb') as f:
                        pdf_base64 = base64.b64encode(f.read()).decode('utf-8')
                    
                    os.remove(output_path)
                    response_data = {
                        'pdf': pdf_base64,
                        'text': extracted_text,
                        'filename': f'ocr_{filename}'
                    }
                    if signature_invalidated:
                        response_data['signature_invalidated'] = True
                    return jsonify(response_data)
            else:
                # Original behavior: return PDF file only
                response = send_file(
                    output_path,
                    mimetype='application/pdf',
                    as_attachment=True,
                    download_name=f'ocr_{filename}'
                )
                
                # Clean up output file after sending
                @response.call_on_close
                def cleanup():
                    try:
                        os.remove(output_path)
                    except:
                        pass
                
                return response
        else:
            # OCR failed and no output was created
            try:
                os.remove(output_path)
            except:
                pass
            
            # Provide specific error messages based on stderr
            stderr = result.stderr
            app.logger.error(f"OCR failed. Return code: {result.returncode}, stderr: {stderr[:1000]}")
            
            if 'DigitalSignatureError' in stderr:
                error_msg = 'PDF has a digital signature - the --invalidate-digital-signatures flag may not be working'
            elif 'The generated PDF is INVALID' in stderr:
                error_msg = 'OCR produced invalid output - input PDF may be corrupted'
            elif 'EncryptedPdfError' in stderr:
                error_msg = 'PDF is encrypted and cannot be processed'
            else:
                error_msg = 'OCR processing failed'
            
            return jsonify({
                'error': error_msg,
                'details': stderr
            }), 500
            
    except subprocess.TimeoutExpired:
        app.logger.error("OCR processing timeout")
        return jsonify({'error': 'OCR processing timeout'}), 504
    except Exception as e:
        app.logger.error(f"Unexpected error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/info', methods=['GET'])
def info():
    """Get OCRmyPDF version and available languages"""
    try:
        version_result = subprocess.run(
            ['ocrmypdf', '--version'],
            capture_output=True,
            text=True
        )
        
        langs_result = subprocess.run(
            ['tesseract', '--list-langs'],
            capture_output=True,
            text=True
        )
        
        # Parse available languages
        langs_output = langs_result.stdout
        languages = [l.strip() for l in langs_output.split('\n')[1:] if l.strip()]
        
        return jsonify({
            'version': version_result.stdout.strip(),
            'available_languages': languages
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
