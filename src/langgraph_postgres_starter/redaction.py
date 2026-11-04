"""Redaction of secrets and personal data (spec §9.10 and §12.1).

``redact`` runs on all text that leaves the process: evidence before it goes into
graph state or to the model, stored run errors, and trace content. It replaces
secrets (keys, tokens, passwords, connection-string passwords, private keys), email
addresses and card numbers with ``[REDACTED]``.

Each rule is specific. Stack traces, hex addresses, commit SHAs, image digests,
UUIDs, pod and node names, exit codes and timestamps come back unchanged, so there
is no generic high-entropy rule.
"""

import re
from collections.abc import Callable

REDACTED = "[REDACTED]"

# PEM private key blocks. A block with no END line (text that was cut off) is
# redacted to the end of the text.
_PRIVATE_KEY = re.compile(
    r"-----BEGIN [A-Z0-9 ]*PRIVATE KEY[A-Z ]*-----"
    r"(?:.*?-----END [A-Z0-9 ]*PRIVATE KEY[A-Z ]*-----|.*)",
    re.DOTALL,
)

# The password in scheme://user:PASSWORD@host. It runs to the last "@" before the
# path, so an unescaped "@" in the password is redacted too.
_URL_PASSWORD = re.compile(
    r"(?<![A-Za-z0-9+.-])([A-Za-z][A-Za-z0-9+.-]*://[^\s:/?#@]*:)[^\s/?#\"]+(?=@)"
)

# The credentials after the scheme in an Authorization header. Without a scheme the
# value is ordinary text, as in "may require authorization: server message".
_AUTH_HEADER = re.compile(
    r"(?i)(\bauthorization\\?[\"']?[ \t]*[:=][ \t]*\\?[\"']?(?:bearer|basic|token)[ \t]+)"
    r"[A-Za-z0-9._~+/=-]+"
)

# A bearer token outside an Authorization header, in any case ("BEARER" too). The
# minimum length keeps ordinary words that follow "Bearer" in prose.
_BEARER = re.compile(r"(?i)(\bbearer[ \t]+)[A-Za-z0-9._~+/=-]{16,}")

# Tokens with a known, distinctive shape.
_KNOWN_TOKENS = re.compile(
    r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b"  # AWS access key ID
    r"|\bgh[pousr]_[A-Za-z0-9]{36,}"  # GitHub token
    r"|\bgithub_pat_[A-Za-z0-9_]{22,}"  # GitHub fine-grained token
    r"|\bxox[abposr]-[A-Za-z0-9-]{10,}"  # Slack token
    r"|\beyJ[A-Za-z0-9_-]+\.eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]*"  # JWT
)

# key=value, key: value and "key": "value" pairs whose key names a secret, for
# example password, DB_PASSWORD, client_secret, access_token or X-Api-Key. Keys such
# as max_tokens or secretName do not match: the secret word must end the key. A key
# right after "/" is the user in a URL (https://x-access-token:...@host), which
# _URL_PASSWORD handles.
# A quoted value is redacted to its closing quote, or to the end of the line if the
# quote is not closed. A bare value is redacted up to whitespace, a quote, ",", ";"
# or "&".
_KEY_VALUE = re.compile(
    r"""
    (?<![A-Za-z0-9_./-])
    (?P<key>
        (?:[A-Za-z0-9_.-]*[_.-])?
        (?:passw(?:or)?d|pwd|passphrase|secret|token
          |(?:api|app|access|secret|private|client|signing|encryption|master)[_-]?key)
    )
    (?P<sep>\\?["']?(?:[ \t]*=(?!=)|:(?!:))[ \t]*)
    (?P<quote>\\?["'])?
    (?P<scheme>(?:bearer|basic)[ \t]+)?
    (?(quote)
        (?:\\.|[^\\\n])*?(?P<close>(?P=quote)|$)
      | [^\s"',;&]+
    )
    """,
    re.IGNORECASE | re.MULTILINE | re.VERBOSE,
)

_EMAIL = re.compile(
    r"(?<![A-Za-z0-9._%+-])[A-Za-z0-9._%+-]+@"
    r"[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)*\.[A-Za-z]{2,}(?![A-Za-z0-9-])"
)

# Card number candidates: 13-19 digits in one run, or any number of digit groups
# split by single spaces or dashes, so one candidate can hold several cards side by
# side. A candidate next to a letter, digit, "-" or decimal point is part of
# something else (a hex address, digest, UUID or decimal number) and is skipped.
_CARD_CANDIDATE = re.compile(
    r"(?<![\w.-])(?:[0-9]{4,6}(?:[ -][0-9]{3,6})+|[0-9]{13,19})(?![\w-]|\.[0-9])"
)
_DIGIT_GROUP = re.compile(r"[0-9]+")
# A card has at most 19 digits, and every group has at least 3, so a card spans at
# most 6 groups.
_MAX_CARD_GROUPS = 6
# Leading digits of the card networks: 3 (Amex, Diners, JCB), 4 (Visa),
# 5 (Mastercard, Maestro), 6 (Discover, UnionPay) and 22-27 (Mastercard). Epoch
# timestamps and compact dates start with 1 or 20, so they do not pass.
_CARD_PREFIX = re.compile(r"[3-6]|2[2-7]")


def _is_card_number(digits: str) -> bool:
    """Return whether ``digits`` has a card length and prefix and a valid Luhn digit."""
    if not 13 <= len(digits) <= 19 or not _CARD_PREFIX.match(digits):
        return False
    total = 0
    for position, char in enumerate(reversed(digits)):
        digit = int(char)
        if position % 2:
            digit = digit * 2 - 9 if digit > 4 else digit * 2
        total += digit
    return total % 10 == 0


def _redact_card(match: re.Match[str]) -> str:
    """Redact each card number in the candidate, from left to right.

    At each digit group, the longest run of up to 6 groups that is a card number is
    redacted, and the scan goes on after it. In "4111 1111 1111 1111 123" the "123"
    stays, and two cards side by side are both redacted. Each group starts at most
    6 runs, so the work grows linearly with the length of the candidate.
    """
    text = match.group()
    groups = list(_DIGIT_GROUP.finditer(text))
    pieces: list[str] = []
    copied_to = 0
    first = 0
    while first < len(groups):
        for last in range(min(first + _MAX_CARD_GROUPS, len(groups)) - 1, first - 1, -1):
            digits = "".join(group.group() for group in groups[first : last + 1])
            if _is_card_number(digits):
                pieces += (text[copied_to : groups[first].start()], REDACTED)
                copied_to = groups[last].end()
                first = last + 1
                break
        else:
            first += 1
    pieces.append(text[copied_to:])
    return "".join(pieces)


def _redact_value(match: re.Match[str]) -> str:
    """Keep the key, separator, quotes and auth scheme; redact the value."""
    return (
        f"{match['key']}{match['sep']}{match['quote'] or ''}{match['scheme'] or ''}"
        f"{REDACTED}{match['close'] or ''}"
    )


# Order matters. Private key blocks go first, as a whole. URL passwords go before
# emails, so "u:pw@db.example.com" keeps its host.
_RULES: tuple[tuple[re.Pattern[str], str | Callable[[re.Match[str]], str]], ...] = (
    (_PRIVATE_KEY, REDACTED),
    (_URL_PASSWORD, r"\g<1>" + REDACTED),
    (_AUTH_HEADER, r"\g<1>" + REDACTED),
    (_BEARER, r"\g<1>" + REDACTED),
    (_KNOWN_TOKENS, REDACTED),
    (_KEY_VALUE, _redact_value),
    (_EMAIL, REDACTED),
    (_CARD_CANDIDATE, _redact_card),
)


def redact(text: str) -> str:
    """Return ``text`` with secrets, email addresses and card numbers replaced.

    Each one becomes ``[REDACTED]``. Text that is already redacted does not change.
    """
    for pattern, replacement in _RULES:
        text = pattern.sub(replacement, text)
    return text


def redact_obj(value: object) -> object:
    """Return a redacted copy of ``value``.

    Strings go through :func:`redact`, also inside dicts, lists and tuples (at any
    depth). Dict keys do not change. A value of any other type is returned as it is,
    as the same object.
    """
    if isinstance(value, str):
        return redact(value)
    if isinstance(value, dict):
        return {key: redact_obj(item) for key, item in value.items()}
    if isinstance(value, list):
        return [redact_obj(item) for item in value]
    if isinstance(value, tuple):
        return tuple(redact_obj(item) for item in value)
    return value
