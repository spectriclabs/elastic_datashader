#!/usr/bin/env python
import argparse
import logging
import os
import socket
import ssl
from typing import Optional

import urllib3
from flask import Flask
from flask_apscheduler import APScheduler

from tms_pixellock_api.helpers.cache import scheduled_cache_check_task
from tms_pixellock_api.helpers.config import Config
from tms_pixellock_api.routes import blueprints


def create_app(app_args: Optional[argparse.Namespace] = None) -> Flask:
    """Application factory for setting up Flask app

    Use factory pattern as shown in:

    https://flask.palletsprojects.com/en/1.1.x/tutorial/factory/
    """
    flask_app = Flask(__name__)

    # Load default settings
    flask_app.config.from_object(Config())

    # Load from configuration file
    if os.environ.get("ELASTIC_DATASHADER_SETTINGS"):
        flask_app.config.from_envvar("ELASTIC_DATASHADER_SETTINGS")

    # Load command-line arguments (if provided)
    if app_args:
        for k, v in vars(app_args).items():
            if k.upper() in flask_app.config:
                flask_app.config[k.upper()] = v
    flask_app.config["SECRET_KEY"] = "CSRFProtectionKey"

    # Limit logging at INFO, reduce if needed for debugging
    if flask_app.config["LOG_LEVEL"]:
        flask_app.logger.setLevel(getattr(logging, flask_app.config["LOG_LEVEL"]))

    flask_app.logger.info("Loaded configuration %s", flask_app.config)
    flask_app.logger.info("Loaded environment %s", os.environ)

    # Register the API
    flask_app.logger.info("Registering API")
    flask_app.register_blueprint(blueprints)

    # If ElasticAPM can be loaded, then attempt to configure
    # if via environment variable.  To install APM
    # run `pip install elastic-apm[flask]` then before
    # running the application set the following environment
    # variables:
    #
    #    ELASTIC_APM_SERVICE_NAME
    #    ELASTIC_APM_SERVER_URL
    #
    # Additional parameters can be found here:
    #    https://www.elastic.co/guide/en/apm/agent/python/current/configuration.html
    try:
        if os.environ.get("ELASTIC_APM_SERVER_URL"):
            from elasticapm.contrib.flask import ElasticAPM

            apm = ElasticAPM(flask_app, logging=logging.ERROR)
    except ImportError:
        apm = None

    scheduler = APScheduler()
    scheduler.init_app(flask_app)
    scheduler.start()
    job_id = "CleanupThread_" + str(os.getpid())
    scheduler.add_job(
        func=scheduled_cache_check_task,
        trigger="interval",
        seconds=5 * 60,
        args=(job_id, flask_app.config.get("CACHE_DIRECTORY")),
        id=job_id,
    )
    return flask_app


def setup_cli_parser() -> argparse.Namespace:
    """Setup the CLI parser and parse arguments

    :return: Parsed arguments
    """
    parser = argparse.ArgumentParser(description="TMS Server with Cache")

    # App configuration
    parser.add_argument(
        "-d",
        "--cache_directory",
        default=Config.CACHE_DIRECTORY,
        help="Directory for tile cache",
    )
    parser.add_argument(
        "-t",
        "--cache_timeout",
        default=Config.CACHE_TIMEOUT,
        help="Cache lifespan in sec",
    )
    parser.add_argument(
        "-e",
        "--elastic",
        default=Config.ELASTIC,
        help="Elasticsearch URL, can be comma separated",
    )
    parser.add_argument(
        "--hostname",
        default=socket.getfqdn(),
        help="node hostname")
    parser.add_argument(
        "-H",
        "--proxy_host",
        default=Config.PROXY_HOST,
        help="Proxy host"
    )
    parser.add_argument(
        "-P",
        "--proxy_prefix",
        default=Config.PROXY_PREFIX,
        help="Proxy prefix"
    )
    parser.add_argument(
        "-k",
        "--tms_key",
        default=Config.TMS_KEY,
        help="TMS key required in header"
    )
    parser.add_argument(
        "--header-file",
        default=Config.HEADER_FILE,
        help="configured headers to include in ES requests",
    )
    parser.add_argument(
        "-W",
        "--whitelist-headers",
        default=Config.WHITELIST_HEADERS,
        help="whitelist headers to pass along",
    )
    parser.add_argument(
        "--debug-tiles",
        default=Config.DEBUG_TILES,
        action="store_true",
        help="render tiles with debug overlay",
    )
    # Development server arguments
    parser.add_argument(
        "--debug",
        default=False,
        action="store_true",
        help="Enable Flask debug mode"
    )
    parser.add_argument(
        "-p",
        "--port",
        default=5000,
        help="Port to run TMS server"
    )
    parser.add_argument(
        "-n",
        "--num_processes",
        default=32,
        type=int,
        help="Number of concurrent Flask processes to run",
    )
    parser.add_argument(
        "--ssl_adhoc",
        default=False,
        action="store_true",
        help="Enable SSL in ad-hoc mode",
    )
    parser.add_argument(
        "-s",
        "--ssl",
        default=False,
        action="store_true",
        help=(
            "Enable SSL, set environment variables to confgure: "
            "SSL_SERVER_KEY, SSL_SERVER_CERT, SSL_CA_CHAIN"
        ),
    )
    return parser.parse_args()


if __name__ == "__main__":
    # Disable warnings
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    urllib3.disable_warnings(urllib3.exceptions.InsecurePlatformWarning)
    urllib3.disable_warnings(urllib3.exceptions.SNIMissingWarning)
    urllib3.disable_warnings(UserWarning)

    # Setup logging for non-Flask items
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("elasticsearch").setLevel(logging.WARN)
    logging.getLogger("urllib3").setLevel(logging.WARN)

    # Setup the Flask App
    args = setup_cli_parser()
    app = create_app(args)

    # Set all the flask arguments as a dictionary
    flask_args = {
        "host": "0.0.0.0",
        "port": args.port,
        "processes": args.num_processes,
        "threaded": False,
    }

    logging.getLogger().setLevel(logging.INFO)

    # Handle Debug
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        flask_args["debug"] = True
        flask_args["processes"] = 1

    # Handle SSL
    if args.ssl_adhoc:
        flask_args["ssl_context"] = "adhoc"
    elif args.ssl:
        flask_args["ssl_context"] = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
        flask_args["ssl_context"].load_verify_locations(os.environ.get("SSL_CA_CHAIN"))
        flask_args["ssl_context"].load_cert_chain(
            os.environ.get("SSL_SERVER_CERT"), os.environ.get("SSL_SERVER_KEY")
        )

    # Run Flask
    app.run(**flask_args)
