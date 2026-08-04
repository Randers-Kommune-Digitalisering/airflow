import logging
from html import escape


def get_log_collector() -> logging.Handler:
    # Internal log collector to capture logs for email report
    class _TaskLogCollector(logging.Handler):
        def __init__(self) -> None:
            super().__init__()
            self.entries: list[tuple[int, str]] = []

        def emit(self, record: logging.LogRecord) -> None:
            self.entries.append((record.levelno, self.format(record)))

    log_collector = _TaskLogCollector()
    log_collector.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)-8s] %(name)s: %(message)s")
    )

    return log_collector


def get_styled_log_html(log_collector: logging.Handler, max_lines: int = 400) -> str:
    level_styles = {
        logging.CRITICAL: "color: #b91c1c; font-weight: 700;",
        logging.ERROR: "color: #d66a6a; font-weight: 700;",
        logging.WARNING: "color: #9aa300; font-weight: 600;",
        logging.INFO: "color: #000000;",
        logging.DEBUG: "color: #000000;",
    }

    def _style_for_level(levelno: int) -> str:
        if levelno >= logging.CRITICAL:
            return level_styles[logging.CRITICAL]
        if levelno >= logging.ERROR:
            return level_styles[logging.ERROR]
        if levelno >= logging.WARNING:
            return level_styles[logging.WARNING]
        if levelno >= logging.INFO:
            return level_styles[logging.INFO]
        return level_styles[logging.DEBUG]

    styled_log_lines = [
        f"<span style='{_style_for_level(levelno)}'>{escape(line)}</span>"
        for levelno, line in log_collector.entries[-max_lines:]
    ]
    return "\n".join(styled_log_lines)
