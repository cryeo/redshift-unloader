import logging

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s / %(name)s] [%(levelname)s] %(message)s"
)

logger = logging.getLogger("redshift-unloader")
logger.setLevel(logging.DEBUG)
