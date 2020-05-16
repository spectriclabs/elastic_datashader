#!/usr/bin/env python3
import os
import socket


class Config(object):
    """
    The default configuration; configuration parameters need
    to be in all upper case to be loaded correctly by
    the flask helpers
    """

    # Configuration that can be modifed by the user
    LOG_LEVEL = os.environ.get("DATASHADER_LOG_LEVEL", None)
    CACHE_DIRECTORY = os.environ.get("DATASHADER_CACHE_DIRECTORY", "./tms-cache/")
    CACHE_TIMEOUT = int(os.environ.get("DATASHADER_CACHE_TIMEOUT", 60 * 60))
    ELASTIC = os.environ.get("DATASHADER_ELASTIC", "http://localhost:9200")
    PROXY_HOST = os.environ.get("DATASHADER_PROXY_HOST", None)
    PROXY_PREFIX = os.environ.get("DATASHADER_PROXY_PREFIX", "")
    TMS_KEY = os.environ.get("DATASHADER_TMS_KEY", None)
    MAX_BINS = int(os.environ.get("DATASHADER_MAX_BINS", 10000))
    MAX_BATCH = int(os.environ.get("DATASHADER_MAX_BATCH", 10000))
    MAX_ELLIPSES_PER_TILE = int(os.environ.get("DATASHADER_MAX_BATCH", 100000))
    HEADER_FILE = os.environ.get("DATASHADER_HEADER_FILE", "./headers.yaml")
    WHITELIST_HEADERS = os.environ.get("DATASHADER_WHITELIST_HEADERS", None)
    DEBUG_TILES = os.environ.get("DATASHADER_DEBUG_TILES", False)
    PORT = None
    HOSTNAME = socket.getfqdn()
