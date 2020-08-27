#!/usr/bin/env bash
REV=`git -C ../kibana rev-parse --short HEAD`
TS=`date +%Y%m%d`

tar -C ../kibana -cvzf datashader_layer_${TS}_${REV}.tar.gz \
	x-pack/legacy/plugins/datashader_layer \
	x-pack/legacy/plugins/maps/common/constants.js \
	x-pack/legacy/plugins/maps/public/layers/sources/all_sources.js \
	x-pack/legacy/plugins/maps/public/selectors/map_selectors.js \
	x-pack/legacy/plugins/maps/public/angular/map_controller.js \
	x-pack/legacy/plugins/maps/public/connected_components/map/mb/view.js \
	x-pack/legacy/plugins/maps/public/connected_components/toolbar_overlay/set_view_control/set_view_control.js \
	src/legacy/server/config/schema.js
