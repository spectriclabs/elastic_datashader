#!/usr/bin/env python3
import json
from pathlib import Path
from pprint import pformat

from flask import current_app, render_template, request, Blueprint

from elastic_datashader.helpers.cache import du, build_layer_info

view_blueprints = Blueprint("views", __name__, template_folder="templates")


@view_blueprints.route("/")
@view_blueprints.route("/index")
def index():
    cache_dir = current_app.config["CACHE_DIRECTORY"]

    # Calc Cache Size
    cache_size = du(cache_dir)

    # Build Layer Info
    return render_template(
        "index.html",
        title="Elastic Data Shader Server",
        cache_size=cache_size,
        layer_info=build_layer_info(cache_dir),
    )


@view_blueprints.route("/parameters", methods=["GET"])
def display_parameters():
    cache_dir = Path(current_app.config["CACHE_DIRECTORY"])
    name = request.args.get("name")
    hash_ = request.args.get("hash")

    template_kwargs = {
        "title": "Elastic Data Shader Server",
        "name": name,
        "hash": hash_,
        "params": {},
    }

    params_json = cache_dir / name / hash_ / "params.json"
    if params_json.exists():
        with params_json.open("r") as f:
            params = json.load(f)
        template_kwargs.update(
            {
                "params": params,
                "generated_params": pformat(params.get("generated_params", {})),
            }
        )

    return render_template("parameters.html", **template_kwargs)
