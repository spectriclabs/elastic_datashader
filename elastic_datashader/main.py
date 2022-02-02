from fastapi import FastAPI

import logging

from .config import config
from .helpers.elastic import verify_datashader_indices
from .helpers.drawing import initialize_custom_color_maps
from .routes import api_blueprints
from .views import view_blueprints

def setup_logging(config: Config) -> None:
    logging.basicConfig(
        level=logging.DEBUG if config.debug else logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    logging.getLogger("elasticsearch").setLevel(logging.WARN)
    logging.getLogger("numba").setLevel(logging.WARN)

logger = logging.getLogger(__name__)
logger.info("Loaded configuration %s", config)
verify_datashader_indices(config.elastic_host)
initialize_custom_color_maps()
app = FastAPI()


def create_app() -> FastAPI:

    # Register the API
    flask_app.logger.info("Registering API")
    flask_app.register_blueprint(api_blueprints)

    flask_app.logger.info("Registering Views")
    flask_app.register_blueprint(view_blueprints)

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

            ElasticAPM(flask_app, logging=logging.ERROR)
    except ImportError:
        pass

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
