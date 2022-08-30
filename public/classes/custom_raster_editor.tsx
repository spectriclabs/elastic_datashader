
import React, { Component } from 'react';
import { EuiCallOut, EuiPanel, htmlIdGenerator } from '@elastic/eui';
import { RenderWizardArguments } from '@kbn/maps-plugin/public';
import { LayerDescriptor, LAYER_TYPE } from '@kbn/maps-plugin/common';
import { CustomRasterSource } from './custom_raster_source';
//import {  getIndexPatternService } from '@kbn/maps-plugin//kibana_services';
export type DatashaderSourceConfig = {
  urlTemplate: string;
  indexTitle: string;
  indexPatternId: string;
  timeFieldName: string;
  geoField: string;
  applyGlobalQuery: boolean;
  applyGlobalTime: boolean;
}
export class CustomRasterEditor extends Component<RenderWizardArguments> {
  componentDidMount() {
    const customRasterLayerDescriptor: LayerDescriptor = {
      id: htmlIdGenerator()(),
      type: LAYER_TYPE.RASTER_TILE,
      sourceDescriptor: CustomRasterSource.createDescriptor({
        urlTemplate:"",//TODO check we can get the datashader server to start by kibana and we know the default url and default port
      } as DatashaderSourceConfig),
      style: {
        type: 'RASTER',
      },
      alpha: 1,
    };
    this.props.previewLayers([customRasterLayerDescriptor]);
  }

  render() {
    return (
      <EuiPanel>
        <EuiCallOut title="Datashader">
          <p>
            Utility layer that visualized location data as a intensity map, or displays ellipses
          </p>
        </EuiCallOut>
      </EuiPanel>
    );
  }
}
