from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, AttrDict, Document, UpdateByQuery
from elasticsearch.exceptions import NotFoundError
import os
import socket
from datetime import datetime

accesslog = "-"
errorlog = "-"

def pre_request(worker, req):
    worker.current_request = req

def worker_abort(worker):
    current_app = worker.app.wsgi()

    es = Elasticsearch(
            current_app.config.get("ELASTIC").split(","),
            verify_certs=False,
            timeout=120,
        )
    doc = Document(
            url=worker.current_request.uri,
            host=socket.gethostname(),
            pid=os.getpid(),
            timestamp=datetime.now(),
            timeout=True,
            error="Hit gunicorn timeout prior to request completion"
        )
    doc.save(using=es, index=".datashader_tiles")