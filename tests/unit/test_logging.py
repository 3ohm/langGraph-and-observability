import contextlib
import io
import json
import logging
import uuid

import structlog

from langgraph_and_observability.logging import configure_logging, get_logger
from langgraph_and_observability.settings import load_settings


def _last_line(capsys) -> dict[str, object]:
    line: dict[str, object] = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    return line


def test_logs_are_json_with_bound_fields(capsys):
    configure_logging(load_settings(env="test"))
    get_logger("t").bind(run_id="r1").info("hello")
    line = _last_line(capsys)
    assert line["event"] == "hello" and line["run_id"] == "r1" and "timestamp" in line
    assert line["level"] == "info" and line["logger"] == "t"


def test_uuid_values_render_as_plain_strings(capsys):
    configure_logging(load_settings(env="test"))
    run_id = uuid.uuid7()
    get_logger("t").info("started", run_id=run_id)
    assert _last_line(capsys)["run_id"] == str(run_id)


def test_stdlib_records_render_as_json_with_context(capsys):
    configure_logging(load_settings(env="test"))
    with structlog.contextvars.bound_contextvars(run_id="r2"):
        logging.getLogger("third.party").warning("disk %s", "full")
    line = _last_line(capsys)
    assert (line["event"], line["level"], line["logger"]) == ("disk full", "warning", "third.party")
    assert line["run_id"] == "r2" and "timestamp" in line


def test_exceptions_render_as_text(capsys):
    configure_logging(load_settings(env="test"))
    try:
        raise RuntimeError("boom")
    except RuntimeError:
        get_logger("t").exception("failed")
    line = _last_line(capsys)
    assert line["level"] == "error"
    assert isinstance(line["exception"], str) and "RuntimeError: boom" in line["exception"]


def test_configuring_twice_writes_each_line_once(capsys):
    settings = load_settings(env="test")
    configure_logging(settings)
    configure_logging(settings)
    get_logger("t").info("once")
    assert len(capsys.readouterr().out.strip().splitlines()) == 1


def test_logs_follow_the_current_stdout():
    configure_logging(load_settings(env="test"))
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        get_logger("t").info("redirected")
    assert json.loads(buffer.getvalue())["event"] == "redirected"
