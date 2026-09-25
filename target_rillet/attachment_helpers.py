from typing import Tuple

_OLE_HEADER = b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"


def resolve_attachment_filename_and_type(
    att_name: str, content: bytes, content_type: str
) -> Tuple[str, str]:
    ct = content_type.lower()

    if ct == "application/pdf" or content[:4] == b"%PDF":
        return f"{att_name}.pdf", "application/pdf"
    if ct in ("image/jpeg", "image/jpg") or content[:2] == b"\xff\xd8":
        return f"{att_name}.jpg", "image/jpeg"
    if ct == "image/png" or content[:8] == b"\x89PNG\r\n\x1a\n":
        return f"{att_name}.png", "image/png"
    if ct == "image/gif" or content[:6] in (b"GIF87a", b"GIF89a"):
        return f"{att_name}.gif", "image/gif"
    if ct == "image/bmp" or content[:2] == b"BM":
        return f"{att_name}.bmp", "image/bmp"
    if ct in ("image/tiff", "image/tif") or content[:4] in (b"II*\x00", b"MM\x00*"):
        return f"{att_name}.tif", "image/tiff"
    if ct in ("application/rtf", "text/rtf") or content[:5] == b"{\\rtf":
        return f"{att_name}.rtf", "application/rtf"
    if ct in ("text/csv", "application/csv"):
        return f"{att_name}.csv", "text/csv"
    if ct in ("text/plain", "text/txt"):
        return f"{att_name}.txt", "text/plain"
    if "wordprocessingml" in ct or ct == (
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    ):
        return (
            f"{att_name}.docx",
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        )
    if ct in ("application/msword",):
        return f"{att_name}.doc", "application/msword"
    if "spreadsheetml" in ct or ct == (
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    ):
        return (
            f"{att_name}.xlsx",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    if ct == "application/vnd.ms-excel":
        return f"{att_name}.xls", "application/vnd.ms-excel"
    if "presentationml" in ct or ct == (
        "application/vnd.openxmlformats-officedocument.presentationml.presentation"
    ):
        return (
            f"{att_name}.pptx",
            "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        )
    if ct == "application/vnd.ms-powerpoint":
        return f"{att_name}.ppt", "application/vnd.ms-powerpoint"
    if ct in (
        "application/vnd.apple.pages",
        "application/x-iwork-pages-sffpages",
    ):
        return f"{att_name}.pages", "application/vnd.apple.pages"
    if ct in (
        "application/vnd.apple.numbers",
        "application/x-iwork-numbers-sffnumbers",
    ):
        return f"{att_name}.numbers", "application/vnd.apple.numbers"

    if content[:8] == _OLE_HEADER:
        name_lower = att_name.lower()
        if name_lower.endswith(".xls") or ct == "application/vnd.ms-excel":
            return f"{att_name}.xls", "application/vnd.ms-excel"
        if name_lower.endswith(".doc") or ct == "application/msword":
            return f"{att_name}.doc", "application/msword"
        if name_lower.endswith(".ppt") or ct == "application/vnd.ms-powerpoint":
            return f"{att_name}.ppt", "application/vnd.ms-powerpoint"

    return f"{att_name}.bin", "application/octet-stream"
