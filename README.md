# Elastic Datashader

Python Jupyter Notebook that shows how to efficently render geospatial data
from ElasticSearch via [Datashader](datashader.org), so that instead of this:

![Kibana](img/kibana.png)

You get this:

![Datashader](img/datashader.png)

![Queries](img/term_colors.png)

This is accomplished by taking a bounding-box query area (potentially the entire
world).

![Bounding Box](img/bbox.png)

Then sub-dividing it into smaller individual queries that can be executed
in parallel at a higher-precision.

![Queries](img/queries.png)

These individual queries are reduced into a single dataframe before being
rendered via Datashader.
