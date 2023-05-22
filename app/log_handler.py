import sys
from loguru import logger

log_format = "{time:YYYY-MM-DDTHH:mm:ss.fff} | {level: ^7} | {file: ^20} | {function: ^20} | {line: >3} | {message}"

logger = logger.opt(colors=True)
logger.remove()
logger.add(
        sys.stderr,
        format=log_format,
        enqueue=True,
        colorize=True,
    )
