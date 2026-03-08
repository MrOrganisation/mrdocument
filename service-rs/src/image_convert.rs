use ::image::GenericImageView;
use printpdf::*;
use tracing::{debug, info, warn};

/// Supported image extensions
const SUPPORTED_EXTENSIONS: &[&str] = &[
    ".jpg", ".jpeg", ".png", ".gif", ".tiff", ".tif", ".bmp", ".webp", ".ppm", ".pgm", ".pbm",
    ".pnm",
];

/// Check if filename has a supported image extension.
pub fn is_supported_image(filename: &str) -> bool {
    let lower = filename.to_lowercase();
    SUPPORTED_EXTENSIONS.iter().any(|ext| lower.ends_with(ext))
}

/// Convert image bytes to PDF.
pub fn image_to_pdf(image_bytes: &[u8], filename: &str) -> Result<Vec<u8>, String> {
    debug!(
        "Converting image to PDF: {} ({} bytes)",
        filename,
        image_bytes.len()
    );

    let lower = filename.to_lowercase();
    let is_jpeg = lower.ends_with(".jpg") || lower.ends_with(".jpeg");

    // For JPEG, embed directly into PDF
    if is_jpeg {
        match embed_jpeg_pdf(image_bytes) {
            Ok(pdf) => {
                info!(
                    "Converted JPEG to PDF: {} -> {} bytes",
                    image_bytes.len(),
                    pdf.len()
                );
                return Ok(pdf);
            }
            Err(e) => {
                warn!("Direct JPEG embedding failed: {}, trying image decode", e);
            }
        }
    }

    // Decode image and convert
    let img = ::image::load_from_memory(image_bytes)
        .map_err(|e| format!("Failed to load image: {}", e))?;

    let (width, height) = img.dimensions();

    // Convert to RGB
    let rgb_img = img.to_rgb8();
    let raw_pixels = rgb_img.into_raw();

    // Create PDF with printpdf
    let dpi: f32 = 150.0;
    let page_width_mm = (width as f32 / dpi) * 25.4;
    let page_height_mm = (height as f32 / dpi) * 25.4;

    let (doc, page1, layer1) = PdfDocument::new(
        "Image",
        Mm(page_width_mm),
        Mm(page_height_mm),
        "Layer 1",
    );

    let current_layer = doc.get_page(page1).get_layer(layer1);

    let image = Image::from(ImageXObject {
        width: Px(width as usize),
        height: Px(height as usize),
        color_space: ColorSpace::Rgb,
        bits_per_component: ColorBits::Bit8,
        interpolate: true,
        image_data: raw_pixels,
        image_filter: None,
        clipping_bbox: None,
        smask: None,
    });

    image.add_to_layer(
        current_layer,
        ImageTransform {
            translate_x: Some(Mm(0.0)),
            translate_y: Some(Mm(0.0)),
            scale_x: Some(page_width_mm / (width as f32 / dpi * 25.4)),
            scale_y: Some(page_height_mm / (height as f32 / dpi * 25.4)),
            ..Default::default()
        },
    );

    let pdf_bytes = doc
        .save_to_bytes()
        .map_err(|e| format!("Failed to save PDF: {}", e))?;

    info!(
        "Converted image to PDF: {} -> {} bytes",
        image_bytes.len(),
        pdf_bytes.len()
    );

    Ok(pdf_bytes)
}

/// Embed a JPEG directly into a minimal PDF.
fn embed_jpeg_pdf(jpeg_bytes: &[u8]) -> Result<Vec<u8>, String> {
    // Decode just to get dimensions
    let img = ::image::load_from_memory(jpeg_bytes)
        .map_err(|e| format!("Failed to decode JPEG: {}", e))?;
    let (width, height) = img.dimensions();

    let dpi: f32 = 150.0;
    let page_w = (width as f32 / dpi) * 25.4;
    let page_h = (height as f32 / dpi) * 25.4;

    // Convert to RGB and use printpdf
    let rgb_img = img.to_rgb8();
    let raw_pixels = rgb_img.into_raw();

    let (doc, page1, layer1) = PdfDocument::new(
        "Image",
        Mm(page_w),
        Mm(page_h),
        "Layer 1",
    );

    let layer = doc.get_page(page1).get_layer(layer1);

    let image = Image::from(ImageXObject {
        width: Px(width as usize),
        height: Px(height as usize),
        color_space: ColorSpace::Rgb,
        bits_per_component: ColorBits::Bit8,
        interpolate: true,
        image_data: raw_pixels,
        image_filter: None,
        clipping_bbox: None,
        smask: None,
    });

    image.add_to_layer(
        layer,
        ImageTransform::default(),
    );

    doc.save_to_bytes()
        .map_err(|e| format!("Failed to create PDF: {}", e))
}
