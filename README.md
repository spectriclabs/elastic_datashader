# Elastic Datashader

## Introduction

[Elastic Datashader](https://github.com/spectriclabs/elastic_datashader) combines
the power of [ElasticSearch](www.elastic.co) with [Datashader](https://datashader.org/).
So you can go from this:

![Kibana Default Heatmap](doc/img/elastic_heatmap.png)

To this:

![Kibana Default Heatmap](doc/img/datashader_heatmap.png)

## Running

### Setup
Poetry takes care of installing dependencies within the virtual environment.  First install poetry.

```sh
python3 -m pip install poetry
```

Now we can create the virtual environment and install dependencies into it with

```sh
poetry install
```

Note that there are extras that can also be installed with `--extras` which are specified below.

### Locally

First enter the virtualenv created by poetry.

```sh
poetry shell
```

#### Uvicorn
First you need to install the `localwebserver` optional extra.

```sh
poetry install --extras localwebserver
```

uvicorn is now available for you within the virtualenv (you can reenter with `poetry shell`).  Note that the log level for the `datashader` logger can be set within the `logging_config.yml` or by setting the `DATASHADER_LOG_LEVEL` environment variable; the latter takes precedence.

```sh
DATASHADER_ELASTIC=http://user:password@localhost:9200 uvicorn elastic_datashader:app --reload --port 6002 --log-config logging_config.yml 
```

### Docker

First build the Docker container by running 'make' within the folder:

```sh
make
```

To run in production mode via Docker+Uvicorn:

```sh
$ docker run -it --rm=true -p 5000:5000 \
    elastic_datashader:0.0.6 \
    --log-level=debug \
    -b :5000 \
    --workers 32 \
    --env DATASHADER_ELASTIC=http://user:passwordt@host:9200 \
    --env DATASHADER_LOG_LEVEL=DEBUG
```

### Running behind NGINX

Run datashader as normal and use the following NGINX configuration snippet:

```
  location /datashader/ {
    proxy_pass http://ip-to-datashader-server:5000/;
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Host $host;
    proxy_set_header X-Forwarded-Server $host;
    proxy_set_header X-Forwarded-Port $server_port;
    proxy_set_header X-Forwarded-Proto $scheme;
  }
```

### Testing
From within the virtualenv (`poetry shell`) just run the following.

```sh
pytest
```

## Tweaks

Datashader layers will be generated faster if Elastic `search.max_buckets` is increase to 65536.

## Kibana

Integration with Kibana Maps can be found [here](https://github.com/spectriclabs/kibana/tree/feat-datashader).  This code
requires changes to code covered via the Elastic License.  It is your responsibility to use this code in compliance with this license.

You can build a Kibana with Elastic-Datashader support:

```sh
cd kibana
make
```

## API

The API is currently provisional and may change in future releases.

### Get Tile

**URL** : `/tms/{index-name}/{z}/{x}/{y}.png`
**Method** : `GET`
**QueryParameter** :

**Required:**

* `geopoint_field=[alphanumeric]` : the field to use for geopoint coordinates.

**Optional:**

* `timestamp_field=[string]` : the field to use for time (default: `@timestamp`)
* `params=[json]` : query/filter parameters from kibana.
* `cmap=[alphanumeric]` : the colorcet map to use (default: `bmy` for heatmap and `glasbey_category10` for colored points)
* `category_field=[alphanumeric]` : the field to be used for coloring points/ellipses
* `category_type=[alphanumeric]` : the type of the category_field (as found in Kibana Index Pattern)
* `category_format=[alphanumeric]` : the format for numeric category fields (in NumeralJS format)
* `ellipses=[boolean]` : if ellipse shapes should be drawn (default: `false`)
* `ellipse_major=[alphanumeric]` : the field that contains the ellipse major axis size
* `ellipse_minor=[alphanumeric]` : the field that contains the ellipse minor axis size
* `ellipse_tilt=[alphanumeric]` : the field that contains the ellipse tilt degrees
* `ellipse_units=[alphanumeric]` : the units for the ellipse axis (one of `majmin_nm`, `semi_majmin_nm`, or `semi_majmin_m`)
* `ellipse_search=[alphanumeric]` : how far to search for ellipse when generating tiles (one of `narrow`, `normal`, or `wide`)
* `spread=[alphanumeric]` : how large points should be rendered (one of `large`, `medium`, `small`, `auto`)
* `span_range=[alphanumeric]` : the dyanmic range to be applied for alpha channel (one of `flat`, `narrow`, `normal`, `wide`, `auto`)
* `resolution=[alphanumeric]` : the aggregation grid size (default: `finest`),

**Params**

```
{
  "lucene_query": "a lucene query"
  "timeFilters": {
     "from": "now-5h"
     "to": "now"
  }
  "filters" : { ... filter information extracted from Kibana ...}
}
```

### Get Legend

**URL** : `/legend/{index-name}/fieldname`
**Method** : `GET`

**Required:**

* `geopoint_field=[alphanumeric]` : the field to use for geopoint coordinates.

**Optional:**

* `timestamp_field=[string]` : the field to use for time (default: `@timestamp`)
* `params=[json]` : query/filter parameters from kibana.
* `category_field=[alphanumeric]` : the field to be used for coloring points/ellipses
* `category_type=[alphanumeric]` : the type of the category_field (as found in Kibana Index Pattern)
* `category_format=[alphanumeric]` : the format for numeric category fields (in NumeralJS format)
* `cmap=[alphanumeric]` : the colorcet map to use (default: `bmy` for heatmap and `glasbey_category10` for colored points)

**Params**

```
{
  "lucene_query": "a lucene query"
  "timeFilters": {
     "from": "now-5h"
     "to": "now"
  }
  "filters" : { ... filter information extracted from Kibana ...}
  "extent": {
    "minLat": 0.0, "maxLat": 0.0,
    "minLon: 0.0, "maxLon: 0.0
  }
}
```

**Returns:**

```
[
  {"key"="xyz", "color"="acolor", "count"=100},
  {"key"="abc", "color"="acolor", "count"=105},
]
```
