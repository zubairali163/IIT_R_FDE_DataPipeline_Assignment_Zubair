import re


def mask_email(e):
    e = (e or "").strip()
    if not re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", e):
        return "INVALID"
    local, domain = e.split("@", 1)
    return local[:2] + "***@" + domain


def mask_phone(p):
    p = (p or "").strip()
    if not re.fullmatch(r"\d{10}", p):
        return "INVALID"
    return p[:2] + "******" + p[-2:]