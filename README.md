Introduction
============

[Elastic Datashader](https://github.com/spectriclabs/elastic_datashader) combines
the power of [ElasticSearch](www.elastic.co) with [Datashader](https://datashader.org/).
So you can go from this:

![Kibana Default Heatmap](doc/img/elastic_heatmap.png)

To this:

![Kibana Default Heatmap](doc/img/datashader_heatmap.png)

Running
============

Setup
--------------------
Running in virtualenv is recommended.

```
$ virtualenv -p python3 env
$ . env/bin/activate
$ pip install -r requirements.txt
```

Locally
--------------------

To run in debug development mode:

```
$ python tms_datashader.py --debug -p 6002 -e http://user:password@host:9200
```

To run in quasi-production mode:

```
$ python tms_datashader.py -p 6002 -e http://user:password@host:9200
```

When running locally you will need patches to be applied against datashader 0.10.0.  These can be applied
into

Docker
--------------------

First build the Docker container by running 'make' within the folder:

```
$ make
```

To run in production mode via Docker+Gunicorn:

```
$ docker run -it --rm=true -p 5000:5000 \
    elastic_datashader:0.0.2 \
    --log-level=debug \
    -b :5000 \
    --workers 32 \
    --env DATASHADER_ELASTIC=http://user:passwordt@host:9200 \
    --env DATASHADER_LOG_LEVEL=DEBUG
```

Elastic APM support
--------------------
(EXPERIMENTAL) To add Elastic APM support:

```
$ pip install elastic-apm[flask]

$ export ELASTIC_APM_SERVICE_NAME=elastic_datashader
$ export ELASTIC_APM_SERVER_URL=http://localhost:7200
```

Then run as normal.  IMPORTANT, this only works in --debug or Gunicorn mode.  The Elastic APM module
does not work correctly with Flask when multiple worker processes are run.

Running behind NGINX
--------------------

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

Testing
--------------------
```
python -m pytest
```

Tweaks
============

Datashader layers will be generated faster if Elastic `search.max_buckets` is increase to 65536.

Kibana
============

Integration with Kibana Maps can be found [here](https://github.com/spectriclabs/kibana/tree/feat-datashader).  This code
requires changes to code covered via the Elastic License.  It is your responsibility to use this code in compliance with this license.

You can build a Kibana with Elastic-Datashader support:

```
$ cd kibana
$ make
```

API
============

The API is currently provisional and may change in future releases.

Get Tile
--------

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

Get Legend
--------

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
