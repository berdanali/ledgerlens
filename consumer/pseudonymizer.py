"""
PII pseudonymization for the landing zone.

Applies SHA-256 + project salt to customer-identifying fields so that
raw personal data never reaches MinIO.  The mapping is deterministic,
meaning the same input always yields the same token — queries that join
on pseudonymized columns (e.g. linking customers to accounts) still work.

Fields pseudonymized per table:
  customers : first_name, last_name, email, national_id, date_of_birth
  accounts  : account_number  (IBAN is a quasi-identifier under GDPR)
"""

import hashlib
from typing import Any

_PII_FIELDS: dict[str, frozenset[str]] = {
    "customers": frozenset(
        {"first_name", "last_name", "email", "national_id", "date_of_birth"}
    ),
    "accounts": frozenset({"account_number"}),
}


def _sha256_token(value: Any, salt: str) -> str:
    raw = f"{salt}:{value}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def pseudonymize(row: dict[str, Any], table_name: str, salt: str) -> dict[str, Any]:
    """Return a copy of *row* with PII fields replaced by SHA-256 tokens."""
    fields = _PII_FIELDS.get(table_name, frozenset())
    if not fields:
        return row
    result = dict(row)
    for field in fields:
        if field in result and result[field] is not None:
            result[field] = _sha256_token(result[field], salt)
    return result
