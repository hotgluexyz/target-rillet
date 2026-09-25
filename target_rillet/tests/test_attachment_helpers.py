from target_rillet.attachment_helpers import resolve_attachment_filename_and_type


def test_keeps_existing_jpeg_suffix():
    filename, content_type = resolve_attachment_filename_and_type(
        "img_67487.jpeg", b"\xff\xd8\xff", "image/jpeg"
    )
    assert filename == "img_67487.jpeg"
    assert content_type == "image/jpeg"


def test_keeps_existing_jpg_suffix():
    filename, _ = resolve_attachment_filename_and_type(
        "img_67487.jpg", b"", "image/jpeg"
    )
    assert filename == "img_67487.jpg"


def test_appends_jpg_when_name_has_no_extension():
    filename, content_type = resolve_attachment_filename_and_type(
        "img_67487", b"\xff\xd8\xff", "image/jpeg"
    )
    assert filename == "img_67487.jpg"
    assert content_type == "image/jpeg"


def test_keeps_equivalent_tiff_suffix():
    filename, _ = resolve_attachment_filename_and_type(
        "scan.tiff", b"", "image/tiff"
    )
    assert filename == "scan.tiff"


def test_keeps_existing_pdf_suffix():
    filename, _ = resolve_attachment_filename_and_type(
        "invoice.pdf", b"%PDF-1.4", "application/pdf"
    )
    assert filename == "invoice.pdf"
