"""
日志配置：将后台日志按日期写入 app/log 目录，当天日志写入当天文件（如 2026-02-26.log）。

``setup_logging`` 可被多处调用（如 ``main`` 与各服务 ``if __name__ == "__main__"``）；
对 root 的 ``DailyFileHandler`` 仅挂载一次，避免同一行被写入文件多遍。
"""
import logging
from datetime import date
from pathlib import Path


LOG_DIR_NAME = "log"
LOG_MSG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
LOG_DATE_FMT = "%Y-%m-%d %H:%M:%S"


def _get_log_dir() -> Path:
    """app/log 目录（与 app 包同级）。"""
    base = Path(__file__).resolve().parent
    return base / LOG_DIR_NAME


def _ensure_log_dir() -> Path:
    log_dir = _get_log_dir()
    log_dir.mkdir(parents=True, exist_ok=True)
    return log_dir


class DailyFileHandler(logging.FileHandler):
    """按日期切换的 FileHandler：当天日志写入 app/log/YYYY-MM-DD.log，跨天后自动切到新文件。"""

    def __init__(self) -> None:
        self._log_dir = _ensure_log_dir()
        self._current_date: date | None = None
        path = self._today_path()
        super().__init__(path, encoding="utf-8")

    def _today_path(self) -> str:
        self._current_date = date.today()
        return str(self._log_dir / f"{self._current_date.isoformat()}.log")

    def emit(self, record: logging.LogRecord) -> None:
        today = date.today()
        if self._current_date is None or today != self._current_date:
            self.close()
            self.baseFilename = self._today_path()
            self.stream = self._open()
        super().emit(record)


def _root_daily_file_handler(root: logging.Logger) -> DailyFileHandler | None:
    for h in root.handlers:
        if isinstance(h, DailyFileHandler):
            return h
    return None


def setup_logging(level: int = logging.INFO) -> None:
    """配置根 logger：输出到控制台 + 按日期写入 app/log/YYYY-MM-DD.log（DailyFileHandler 幂等）。"""
    root = logging.getLogger()
    root.setLevel(level)
    formatter = logging.Formatter(LOG_MSG_FORMAT, datefmt=LOG_DATE_FMT)

    # 控制台（排除 FileHandler：FileHandler 也是 StreamHandler 子类）
    if not any(
        isinstance(h, logging.StreamHandler) and not isinstance(h, logging.FileHandler)
        for h in root.handlers
    ):
        console = logging.StreamHandler()
        console.setLevel(level)
        console.setFormatter(formatter)
        root.addHandler(console)

    # 按日期写入 app/log（仅挂载一个 DailyFileHandler）
    existing_daily = _root_daily_file_handler(root)
    if existing_daily is not None:
        existing_daily.setLevel(level)
        existing_daily.setFormatter(formatter)
    else:
        try:
            file_handler = DailyFileHandler()
            file_handler.setLevel(level)
            file_handler.setFormatter(formatter)
            root.addHandler(file_handler)
        except OSError as e:
            root.warning("Could not create daily log file in app/log: %s", e)
