from json import dumps

from elasticsearch import Elasticsearch
from fastapi import APIRouter, Response

from ..config import config
from ..elastic import hosts_url_to_nodeconfig

router = APIRouter(
    prefix="/indices",
    tags=["indices"],
    responses={404: {"description": "Not found"}},
)

@router.get("")
async def retrieve_indices():
    es = Elasticsearch(
        hosts_url_to_nodeconfig(config.elastic_hosts),
        verify_certs=False,
        timeout=120)
    aliases = es.indices.get_alias("*")  # pylint: disable=E1121
    indices = [idx for idx in sorted(aliases) if not idx.startswith(".")]
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
        hosts_url_to_nodeconfig(config.elastic_hosts), verify_certs=False, timeout=120
    )
    field_caps = es.field_caps(  # pylint: disable=E1121,E1123
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
        hosts_url_to_nodeconfig(config.elastic_hosts), verify_certs=False, timeout=120
    )
    index_mapping = es.indices.get_mapping(index)  # pylint: disable=E1121
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
