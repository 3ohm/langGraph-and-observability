import pytest

from langgraph_and_observability.redaction import REDACTED, redact, redact_obj

# The table from the plan (Task 5). Outputs must match character for character.
TABLE = [
    ("key=AKIAIOSFODNN7EXAMPLE x", "key=[REDACTED] x"),
    ("Authorization: Bearer abc.def.ghi", "Authorization: Bearer [REDACTED]"),
    ("password=hunter2 next", "password=[REDACTED] next"),
    ("postgresql://u:s3cr3t@db/x", "postgresql://u:[REDACTED]@db/x"),
    ("mail bob@example.com now", "mail [REDACTED] now"),
    ("card 4111 1111 1111 1111", "card [REDACTED]"),
    ("-----BEGIN PRIVATE KEY-----\n…\n-----END PRIVATE KEY-----", "[REDACTED]"),
    (
        "at com.x.Foo.bar(Foo.java:42) 0x7ffd1234abcd",
        "at com.x.Foo.bar(Foo.java:42) 0x7ffd1234abcd",
    ),
]

# Other forms of the same secrets, as they show up in logs, events, code and errors.
MORE_SECRETS = [
    ('{"password": "hunter2", "user": "bob"}', '{"password": "[REDACTED]", "user": "bob"}'),
    ("{'db_password': 'p w d'}", "{'db_password': '[REDACTED]'}"),
    ('log {\\"token\\":\\"abc123\\"}', 'log {\\"token\\":\\"[REDACTED]\\"}'),
    ('password: "never closed', 'password: "[REDACTED]'),
    ("DB_PASSWORD=s3cr3t\nAPP_ENV=prod", "DB_PASSWORD=[REDACTED]\nAPP_ENV=prod"),
    ("X-Api-Key: abc123", "X-Api-Key: [REDACTED]"),
    ('"x-auth-token": "Bearer abc"', '"x-auth-token": "Bearer [REDACTED]"'),
    ("Server=db;Uid=app;Pwd=s3cret;Database=x", "Server=db;Uid=app;Pwd=[REDACTED];Database=x"),
    ("AWS_SECRET_ACCESS_KEY = wJalrXUtnFEMI/K7MDENG", "AWS_SECRET_ACCESS_KEY = [REDACTED]"),
    ("Authorization: Basic dXNlcjpwYXNz", "Authorization: Basic [REDACTED]"),
    ("curl -H 'authorization: bearer tok123'", "curl -H 'authorization: bearer [REDACTED]'"),
    ("sent Bearer 0123456789abcdefXYZ", "sent Bearer [REDACTED]"),
    ("sent BEARER 0123456789abcdefXYZ", "sent BEARER [REDACTED]"),
    ("sent bEaReR 0123456789abcdefXYZ", "sent bEaReR [REDACTED]"),
    ("token ghp_" + "a" * 36, "token [REDACTED]"),
    (
        "jwt eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0In0.c2lnbmF0dXJl end",
        "jwt [REDACTED] end",
    ),
    ("redis://:p@ss@cache:6379/0", "redis://:[REDACTED]@cache:6379/0"),
    (
        "clone https://x-access-token:ghs_abc@github.com/o/r.git",
        "clone https://x-access-token:[REDACTED]@github.com/o/r.git",
    ),
    ("card 4111111111111111.", "card [REDACTED]."),
    ("card 4111-1111-1111-1111", "card [REDACTED]"),
    ("amex 3782 822463 10005", "amex [REDACTED]"),
    ("card 4111 1111 1111 1111 123 cvv", "card [REDACTED] 123 cvv"),
    ("cards 4111 1111 1111 1111 5500 0000 0000 0004", "cards [REDACTED] [REDACTED]"),
    ("cards 4111-1111-1111-1111-5500-0000-0000-0004", "cards [REDACTED]-[REDACTED]"),
    (
        "key:\n-----BEGIN RSA PRIVATE KEY-----\nMIIEowIBAAKCAQEA\n"
        "-----END RSA PRIVATE KEY-----\nok",
        "key:\n[REDACTED]\nok",
    ),
    ("cut -----BEGIN OPENSSH PRIVATE KEY-----\nb3BlbnNzaC1rZXk", "cut [REDACTED]"),
]

# Text that later tasks put through redact() and that must come back unchanged.
NOT_SECRETS = [
    "\tat com.example.orders.OrderService.place(OrderService.java:118)\n"
    "\tat java.base/java.lang.Thread.run(Thread.java:1583)",
    'Traceback (most recent call last):\n  File "/app/orders/service.py", line 42, in place\n'
    '    raise ValueError("bad order")\nValueError: bad order',
    "goroutine 1 [running]:\nmain.main()\n\t/app/main.go:12 +0x1d\nexit status 2",
    "    at Object.<anonymous> (/app/index.js:10:15)\n"
    "    at Module._compile (node:internal/modules/cjs/loader:1554:14)",
    "#0  0x00007f8b2c3d4e5f in raise () from /lib64/libc.so.6\n"
    "#1  0x0000555555554000 in main () at main.c:7",
    "commit 9fceb02d0ae598e95dc970b74767f19372d61af8",
    # Starts with a Luhn-valid card number.
    "commit 4539148803436467a1b2c3d4e5f60718293a4b5c",
    "image ghcr.io/acme/orders@sha256:4111111111111111" + "abcdef0123456789" * 3,
    "run 0190f3a2-7b1c-7d4e-8f00-123456789012 and 12345678-1234-7234-8234-123456789012",
    "pod orders-7d4b9c-x2k8f on node ip-10-0-1-23.ec2.internal",
    "container exited with code 137 (OOMKilled)",
    # Both epoch values are Luhn-valid.
    "2026-11-03T12:34:56.789Z ts_ms=1730600000005 ts_ns=1730600000000000005",
    "p99=0.4111111111111111 seconds",
    'MountVolume.SetUp failed for volume "db-secret" : secret "db-secret" not found',
    "pull access denied, repository does not exist or may require authorization: "
    "server message: insufficient_scope",
    "card 4111 1111 1111 1112",
]


@pytest.mark.parametrize(("text", "expected"), TABLE)
def test_redact_table(text, expected):
    assert redact(text) == expected


@pytest.mark.parametrize(("text", "expected"), MORE_SECRETS)
def test_redact_more_formats(text, expected):
    assert redact(text) == expected


@pytest.mark.parametrize("text", NOT_SECRETS)
def test_no_false_positives(text):
    assert redact(text) == text


@pytest.mark.parametrize("text", [expected for _, expected in TABLE + MORE_SECRETS])
def test_redact_is_idempotent(text):
    assert redact(text) == text


def test_redact_obj_nested():
    marker = object()
    value = {
        "log": "password=hunter2 boom",
        "items": ["mail bob@example.com now", ("key=AKIAIOSFODNN7EXAMPLE x", 3)],
        "bob@example.com": None,
        "obj": marker,
    }

    result = redact_obj(value)

    assert result == {
        "log": f"password={REDACTED} boom",
        "items": [f"mail {REDACTED} now", (f"key={REDACTED} x", 3)],
        "bob@example.com": None,
        "obj": marker,
    }
    assert isinstance(result, dict)
    assert type(result["items"][1]) is tuple
    assert result["obj"] is marker
    assert value["log"] == "password=hunter2 boom"
    assert redact_obj(marker) is marker
