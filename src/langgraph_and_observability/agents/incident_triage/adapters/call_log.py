"""The fake call log: one line per adapter call, so tests can count calls (spec §11, P11).

With ``APP_FAKE_CALL_LOG_DIR`` set, the adapters that ``build_adapters`` makes for a
run append one line per call to ``<APP_FAKE_CALL_LOG_DIR>/<run_id>.log``. The file
outlives the worker process, so an end-to-end test can see which calls a restarted run
repeated. A line is ``<adapter>.<method>``, such as ``k8s.get_pod`` or
``sink.log.send``; a call that waits also writes ``<adapter>.<method>:start`` before it
waits. ``fixtures/FORMAT.md`` lists every line.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Self
from uuid import UUID

from langgraph_and_observability.settings import Settings


@dataclass(frozen=True)
class CallLog:
    """Appends call lines to the file ``path``. Without a path it writes nothing."""

    path: Path | None = None

    @classmethod
    def for_run(cls, settings: Settings, run_id: UUID | None) -> Self:
        """The run's call log: ``fake_call_log_dir / f"{run_id}.log"``, when both are set."""
        if settings.fake_call_log_dir is None or run_id is None:
            return cls()
        return cls(settings.fake_call_log_dir / f"{run_id}.log")

    def record(self, line: str) -> None:
        """Append ``line``. The first line creates the file and its folder."""
        if self.path is None:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8") as file:
            file.write(line + "\n")


# The call log that writes nothing: the default of every adapter.
NO_CALL_LOG = CallLog()
