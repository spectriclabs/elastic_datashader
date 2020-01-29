#! /bin/bash
python /opt/elastic_datashader/tms_pixellock.py -e $ELASTIC_CREDENTIALS -f /opt/elastic_datashader/tms-cache/index_config.yaml -d /opt/elastic_datashader/tms-cache --debug
