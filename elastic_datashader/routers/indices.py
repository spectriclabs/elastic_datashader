from json import dumps
from logging import getLogger

from elasticsearch import Elasticsearch
from fastapi import APIRouter, Response

from ..config import config

logger = getLogger(__name__)

router = APIRouter(
    prefix="/indices",
    tags=["indices"],
    responses={404: {"description": "Not found"}},
)

@router.get("")
async def retrieve_indices():
    es = Elasticsearch(
        config.elastic_hosts.split(","),
        verify_certs=False,
        timeout=120)
    indices = [idx for idx in sorted(es.indices.get_alias("*")) if not idx.startswith(".")]
    indices_json = dumps({"indices": indices})
    return Response(
        indices_json,
        status_code=200,
        headers={
            "Cache-Control": "max-age=60",
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        }
    )

@router.get("/{index}/field_caps")
async def retrieve_field_caps(index: str):
    es = Elasticsearch(
        config.elastic_hosts.split(","), verify_certs=False, timeout=120
    )
    field_caps = es.field_caps(  # pylint: disable=E1123
        index,
        fields='*',
        ignore_unavailable=True,
    )

    response_json = dumps(field_caps)
    return Response(
        response_json,
        status_code=200,
        headers={
            "Cache-Control": "max-age=60",
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        }
    )

@router.get("/{index}/mapping")
async def retrieve_index_mapping(index: str):
    es = Elasticsearch(
        config.elastic_hosts.split(","), verify_certs=False, timeout=120
    )
    index_mapping = es.indices.get_mapping(index)
    mapping = [
        {"name": field, "type": props["type"]}
        for field, props in index_mapping[index]["mappings"]["properties"].items()
    ]

    indices_json = dumps({"mapping": mapping})
    return Response(
        indices_json,
        status_code=200,
        headers={
            "Cache-Control": "max-age=60",
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        }
    )
