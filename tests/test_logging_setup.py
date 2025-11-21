import logging
from app.common.logging_setup import setup_logging


def test_setup_logging():
    """Test that logging is configured correctly."""
    logger = setup_logging(level="DEBUG", component="test_component")

    assert logger.name == "test_component"
    assert logger.level == logging.DEBUG
    assert len(logger.handlers) == 1

    handler = logger.handlers[0]
    assert isinstance(handler, logging.StreamHandler)

    formatter = handler.formatter
    assert isinstance(formatter, logging.Formatter)
    assert formatter._fmt == "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
