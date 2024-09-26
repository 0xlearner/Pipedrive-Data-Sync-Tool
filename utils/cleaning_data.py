import re


def clean_phone(phone):
    """Clean and validate phone number."""
    if not phone:
        return None
    phone = re.sub(r"\D", "", phone)
    return phone if len(phone) >= 10 else None


def clean_email(email):
    """Clean and validate email address."""
    if not email:
        return None
    email = email.strip()
    email_regex = re.compile(r"[^@]+@[^@]+\.[^@]+")
    return email if email_regex.match(email) else None


def clean_benefit_id(benefit_id):
    """Clean and validate benefit ID by removing 'PURL' and other unnecessary characters."""
    # Strip leading/trailing whitespaces
    benefit_id = str(benefit_id).strip()
    # Remove the 'PURL' prefix if it exists
    if benefit_id.startswith("PURL "):
        benefit_id = benefit_id.replace("PURL ", "")
    # Further cleaning (e.g., if you have other prefixes to remove, adjust here)
    return benefit_id
