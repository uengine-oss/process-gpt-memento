"""CWD에 로그 파일 생성. 재시작마다 truncate. stdout/stderr도 Tee."""
from __future__ import annotations

import io
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import IO, Callable, Optional


_HANDLER: Optional[logging.Handler] = None


class _FlushingStreamHandler(logging.StreamHandler):
    """매 emit 후 명시적으로 flush — Windows 텍스트 모드 line-buffer 우회."""

    def emit(self, record: logging.LogRecord) -> None:  # noqa: D401
        super().emit(record)
        try:
            self.stream.flush()
        except Exception:
            pass


def _default_ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S ")


class _Tee:
    """매 라인 시작에 타임스탬프 prefix — print() 호출도 시간이 찍히게."""

    def __init__(self, *streams: IO[str], prefix_factory: Optional[Callable[[], str]] = None) -> None:
        self._streams = streams
        self._prefix_factory = prefix_factory
        self._at_line_start = True

    def _render(self, data: str) -> str:
        if self._prefix_factory is None or not data:
            return data
        out: list[str] = []
        i = 0
        while i < len(data):
            if self._at_line_start:
                out.append(self._prefix_factory())
                self._at_line_start = False
            j = data.find("\n", i)
            if j == -1:
                out.append(data[i:])
                break
            out.append(data[i:j + 1])
            self._at_line_start = True
            i = j + 1
        return "".join(out)

    def write(self, data: str) -> int:
        rendered = self._render(data)
        for s in self._streams:
            try:
                s.write(rendered)
                s.flush()
            except Exception:
                pass
        return len(data)

    def flush(self) -> None:
        for s in self._streams:
            try:
                s.flush()
            except Exception:
                pass

    def isatty(self) -> bool:
        try:
            return self._streams[0].isatty()
        except Exception:
            return False


def setup_file_logging(file_name: str = "memento.log", *, capture_stdout: bool = True) -> Optional[Path]:
    """파일 1개를 열어 logging StreamHandler + stdout/stderr Tee가 함께 사용한다.

    파일을 두 번 열면 두 핸들의 버퍼/append 위치가 어긋나 로그가 깨진다.
    중복 호출 시 한 번만 셋업하도록 idempotent 가드 — 자식 프로세스가
    잘못 진입하더라도 파일 truncate를 막는다.
    """
    global _HANDLER
    if _HANDLER is not None:
        return None

    log_path = Path(file_name)
    try:
        # Windows text-mode line-buffering이 신뢰가 안 됨 → unbuffered binary 위에
        # TextIOWrapper(write_through=True, line_buffering=True)를 직접 얹어 매 write가
        # OS까지 즉시 도달하도록 한다.
        raw = open(log_path, "wb", buffering=0)
        log_file = io.TextIOWrapper(
            raw, encoding="utf-8", line_buffering=True, write_through=True
        )
    except Exception as e:
        print(f"[logging] 로그 파일 오픈 실패({log_path}): {e}", file=sys.stderr)
        return None

    handler = _FlushingStreamHandler(log_file)
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(
        logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )

    root = logging.getLogger()
    if root.level in (0, logging.WARNING):
        root.setLevel(logging.INFO)
    root.addHandler(handler)
    _HANDLER = handler

    if capture_stdout:
        try:
            sys.stdout = _Tee(sys.__stdout__, log_file, prefix_factory=_default_ts)
            sys.stderr = _Tee(sys.__stderr__, log_file, prefix_factory=_default_ts)
        except Exception as e:
            print(f"[logging] stdout/stderr Tee 실패: {e}", file=sys.stderr)

    return log_path


def attach_to_uvicorn_loggers() -> None:
    """uvicorn 자체 로거(propagate=False)에도 핸들러 부착.

    uvicorn은 startup 시 dictConfig로 자기 로거 핸들러를 새로 셋업하므로
    FastAPI startup 이벤트(=uvicorn dictConfig 이후)에서 호출해야 한다.
    """
    if _HANDLER is None:
        return
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        lg = logging.getLogger(name)
        if _HANDLER not in lg.handlers:
            lg.addHandler(_HANDLER)
