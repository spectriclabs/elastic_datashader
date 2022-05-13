import logging

logger = logging.getLogger("datashader")
logging.getLogger("elasticsearch").setLevel(logging.WARN)
logging.getLogger("numba").setLevel(logging.WARN)
