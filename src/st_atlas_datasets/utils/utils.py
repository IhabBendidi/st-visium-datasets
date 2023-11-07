import logging
import re

from rich.logging import RichHandler

logger = logging.getLogger(__name__)


def camelcase_to_snakecase(name: str) -> str:
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler()],
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
