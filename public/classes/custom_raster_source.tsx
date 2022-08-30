/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

import { ReactElement } from 'react';
import {DatashaderStyleEditor, DatashaderStylePropertiesDescriptor, DATASHADER_STYLES} from './ui/datashader_style'
import { FieldFormatter, MIN_ZOOM, MAX_ZOOM, FIELD_ORIGIN, } from '@kbn/maps-plugin/common';

import type {
  AbstractESSourceDescriptor,
  Attribution,
  DataFilters,
  DataRequestMeta,
  Timeslice
} from '@kbn/maps-plugin/common/descriptor_types';

import { PreIndexedShape } from '@kbn/maps-plugin/common/elasticsearch_util';
import type {
  IField,
  ImmutableSourceProperty,
  SourceEditorArgs,
} from '@kbn/maps-plugin/public';


//import { AbstractSource } from '@kbn/maps-plugin/public/classes/sources/source';
//import { XYZTMSSource } from '@kbn/maps-plugin/public/classes/sources/xyz_tms_source';
//import { XYZTMSSource } from '../../../../x-pack/plugins/maps/public/classes/sources/xyz_tms_source';
import { ITMSSource } from '@kbn/maps-plugin/public/classes/sources/tms_source';
import React from 'react';
import { DataViewField,DataView } from '@kbn/data-views-plugin/common';
import { fromKueryExpression, luceneStringToDsl, toElasticsearchQuery } from '@kbn/es-query';
import { getIndexPatternService } from '../kibana_services';
//import { ESDocField } from '@kbn/maps-plugin/public/classes/fields/es_doc_field';
import { i18n } from '@kbn/i18n';
import { FieldFormat } from '@kbn/field-formats-plugin/common';
import { OnSourceChangeArgs } from '@kbn/maps-plugin/public/classes/sources/source';
import { AbstractField } from './fields/field';
import _ from 'lodash';
const NUMBER_DATA_TYPES = [ "number" ]
export const CATEGORICAL_DATA_TYPES = ['string', 'ip', 'boolean'];
export type CustomRasterSourceDescriptor = AbstractESSourceDescriptor & {
  urlTemplate: string,
  indexTitle: string,
  timeFieldName: string,
  attributionText: string,
  attributionUrl: string,
  indexPatternId: string,
  geoField: string,

}& DatashaderStylePropertiesDescriptor;
export type DatashaderSourceConfig = {
  urlTemplate: string;
  indexTitle: string;
  indexPatternId: string;
  timeFieldName: string;
  geoField: string;
  applyGlobalQuery: boolean;
  applyGlobalTime: boolean;
} 

var defaultStyle = {
  [DATASHADER_STYLES.COLOR_RAMP_NAME]: "bmy",
  [DATASHADER_STYLES.COLOR_KEY_NAME]: "glasbey_light",
  [DATASHADER_STYLES.SPREAD]: "auto",
  [DATASHADER_STYLES.SPAN_RANGE]: "normal",
  [DATASHADER_STYLES.GRID_RESOLUTION]: "finest",
  [DATASHADER_STYLES.MODE]: "heat",
  [DATASHADER_STYLES.CATEGORY_FIELD]: "",
  [DATASHADER_STYLES.CATEGORY_FIELD_TYPE]: null,
  [DATASHADER_STYLES.CATEGORY_FIELD_PATTERN]: null,
  [DATASHADER_STYLES.SHOW_ELLIPSES]: false,
  [DATASHADER_STYLES.USE_HISTOGRAM]: undefined,
  [DATASHADER_STYLES.ELLIPSE_MAJOR_FIELD]: "",
  [DATASHADER_STYLES.ELLIPSE_MINOR_FIELD]: "",
  [DATASHADER_STYLES.ELLIPSE_TILT_FIELD]: "",
  [DATASHADER_STYLES.ELLIPSE_UNITS]: "semi_majmin_nm",
  [DATASHADER_STYLES.ELLIPSE_SEARCH_DISTANCE]: "normal",
  [DATASHADER_STYLES.ELLIPSE_THICKNESS]: 0,
  [DATASHADER_STYLES.MANUAL_RESOLUTION]: false,
} as DatashaderStylePropertiesDescriptor
export class CustomRasterSource  implements ITMSSource {
  static type = "CUSTOM_RASTER";

  readonly _descriptor: CustomRasterSourceDescriptor;
  indexPattern: any;
  static createDescriptor(settings:DatashaderSourceConfig): CustomRasterSourceDescriptor {
    return {
      urlTemplate: settings.urlTemplate,
      indexTitle: settings.indexTitle,
      timeFieldName: settings.timeFieldName,
      type: CustomRasterSource.type,
      indexPatternId: settings.indexPatternId,
      geoField: settings.geoField,
      applyGlobalQuery: settings.applyGlobalQuery,
      applyGlobalTime: settings.applyGlobalTime, 
      ...defaultStyle
    } as CustomRasterSourceDescriptor
  }

  constructor(sourceDescriptor: CustomRasterSourceDescriptor) {
    this._descriptor = sourceDescriptor;
  }

  cloneDescriptor(): CustomRasterSourceDescriptor {
    return {
      ...this._descriptor,
    };
  }

  async supportsFitToBounds(): Promise<boolean> {
    return false;
  }

  canSkipSourceUpdate(): boolean {
    console.log("here lets do an update!")
    return false
  }


  /**
   * return list of immutable source properties.
   * Immutable source properties are properties that can not be edited by the user.
   */
  async getImmutableProperties(): Promise<ImmutableSourceProperty[]> {
    return [];
  }
  async getPreIndexedShape(properties: any): Promise<PreIndexedShape | null> {
    return null;
  }
  getType(): string {
    return this._descriptor.type;
  }

  async getDisplayName(): Promise<string> {
    return '';
  }

  getAttributionProvider(): (() => Promise<Attribution[]>) | null {
    return null;
  }

  isFieldAware(): boolean {
    return true;
  }

  isGeoGridPrecisionAware(): boolean {
    return true;
  }

  isQueryAware(): boolean {
    return true;
  }
  getGeoFieldName(): string {
    return this._descriptor.geoField || '';
  }

  getGeoField() {
    return this._descriptor.geoField;
  }

  getFieldNames(): string[] {
    return [];
  }

  renderSourceSettingsEditor(sourceEditorArgs: SourceEditorArgs): ReactElement<any> | null {
    return (<DatashaderStyleEditor handlePropertyChange={(settings: Partial<DatashaderStylePropertiesDescriptor>): void => {
      //throw new Error('Function not implemented.');
      let args = Object.entries(settings).map(v=>({propName:v[0],value:v[1]} as OnSourceChangeArgs))
      //let args = Object.keys(settings).map(key=>({propName:key,value:settings[key] as any} as OnSourceChangeArgs)) 
      sourceEditorArgs.onChange(...args)
    } } layer={this} properties={this._descriptor}/>);
    
  }

  getApplyGlobalQuery(): boolean {
    return true;
  }

  getApplyGlobalTime(): boolean {
    return true;
  }

  getApplyForceRefresh(): boolean {
    return true;
  }

  createField({ fieldName }: { fieldName: string}): AbstractField {
return new AbstractField({
      fieldName,
      source: this,
      origin: FIELD_ORIGIN.SOURCE,
    });

  }

  async getCategoricalFields(): Promise<IField[]> {
    try {
      const indexPattern = await this.getIndexPattern();
      const aggFields: DataViewField[] = [];
      
      CATEGORICAL_DATA_TYPES.forEach(dataType => {
        indexPattern.fields.getByType(dataType).forEach((field: any) => {
          if (field.aggregatable) {
            aggFields.push(field);
          }
        });
      });
      
      NUMBER_DATA_TYPES.forEach(dataType => {
        indexPattern.fields.getByType(dataType).forEach((field:any) => {
          aggFields.push(field);
        });
      });

      return aggFields.map((field: any) => {
        return this.createField({ fieldName: field.name }) as IField;
      });
    } catch (error) {
      return [];
    }
  }

  async getNumberFields() {
    try {
      const indexPattern = await this.getIndexPattern();
      const numberFields: DataViewField[] = [];
      
      NUMBER_DATA_TYPES.forEach(dataType => {
        indexPattern.fields.getByType(dataType).forEach((field:any) => {
          numberFields.push(field);
        });
      });
      return numberFields.map((field: DataViewField) => {
        return this.createField({ fieldName: field.name });
      });
    } catch (error) {
      return [];
    }
  }

  getIndexPatternIds(): string[] {
    return [this._descriptor.indexPatternId];
  }
  async getIndexPattern(): Promise<DataView> {
    if (this.indexPattern) {
      return this.indexPattern;
    }

    try {
      this.indexPattern = await getIndexPatternService().get(this._descriptor.indexPatternId);
      return this.indexPattern;
    } catch (error) {
      throw new Error(
        i18n.translate('xpack.maps.source.esSource.noIndexPatternErrorMessage', {
          defaultMessage: `Unable to find Index pattern for id: {indexPatternId}`,
          values: { indexPatternId: this._descriptor.indexPatternId },
        })
      );
    }
  }

  async getFieldFormatter(field: IField): Promise<FieldFormat | null> {
    let indexPattern;

    try {
      indexPattern = await this.getIndexPattern();
    } catch (error) {
      return null;
    }

    const fieldFromIndexPattern = indexPattern.fields.getByName(field.getRootName());
    
    if (!fieldFromIndexPattern) {
      return null;
    }

    return indexPattern.getFormatterForField(fieldFromIndexPattern);
  }

  getQueryableIndexPatternIds(): string[] {
    return [];
  }

  getGeoGridPrecision(zoom: number): number {
    return 0;
  }

  isESSource(): boolean {
    return true;
  }

  // Returns function used to format value
  async createFieldFormatter(field: IField): Promise<FieldFormatter | null> {
    return null;
  }

  async getValueSuggestions(field: IField, query: string): Promise<string[]> {
    return [];
  }

  async isTimeAware(): Promise<boolean> {
    return true;
  }

  isFilterByMapBounds(): boolean {
    return true;
  }

  getMinZoom(): number {
    return MIN_ZOOM;
  }

  getMaxZoom(): number {
    return MAX_ZOOM;
  }

  async getLicensedFeatures(): Promise<[]> {
    return [];
  }

  getUpdateDueToTimeslice(prevMeta: DataRequestMeta, timeslice?: Timeslice): boolean {
    return true;
  }
  getStyleUrlParams(data: any) {
    let urlParams = "";

    // the current implementation of auto is too slow, so remove it
    let span = data.spanRange;
    //if (span === "auto") {
    //  span = "normal";
    //}

    urlParams = urlParams.concat(
        "&span=", span,
    )

    if (data.showEllipses &&
      data.ellipseMajorField &&
      data.ellipseMinorField &&
      data.ellipseTiltField) {
      urlParams = urlParams.concat(
        "&ellipses=", data.showEllipses.toString(),
        "&ellipse_major=", data.ellipseMajorField,
        "&ellipse_minor=", data.ellipseMinorField,
        "&ellipse_tilt=", data.ellipseTiltField,
        "&ellipse_units=", data.ellipseUnits,
        "&ellipse_search=", data.ellipseSearchDistance,
        "&spread=", data.ellipseThickness.toString(),
      );
    } else {
      urlParams = urlParams.concat(
        "&spread=", data.spread,
        "&resolution=", data.gridResolution
      )
    }

    if (data.mode === "heat") {
      urlParams = urlParams.concat(
        "&cmap=", data.colorRampName,
      );
    } else if (data.mode === "category" &&
              data.categoryField &&
              data.categoryFieldType &&
              data.colorKeyName
    ) {
      urlParams = urlParams.concat(
        "&category_field=", data.categoryField,
        "&category_type=", data.categoryFieldType,
        "&cmap=", data.colorKeyName,
      );
      if (data.useHistogram === true) {
        urlParams = urlParams.concat(
          "&category_histogram=true" 
        );
      } else if (data.useHistogram === false) {
        urlParams = urlParams.concat(
          "&category_histogram=false" 
        );        
      }

      if (data) {
        let pattern = (data.categoryFieldMeta && data.categoryFieldMeta.spec.format) ? data.categoryFieldMeta.spec.format.params.pattern : null;
        if (!pattern && data.categoryFieldFormatter) {
          pattern = data.categoryFieldFormatter.getParamDefaults().pattern
        }
        urlParams = urlParams.concat(
          "&category_pattern=", pattern
        );
      } else if (data.categoryFieldPattern) {
        urlParams = urlParams.concat(
          "&category_pattern=", data.categoryFieldPattern
        );
      }
    }

    return urlParams;
  }
  async getUrlTemplate(dataFilters:DataFilters): Promise<string> {
    try {
    let data = {...dataFilters,...this._descriptor}
    console.log(dataFilters)
    console.log(this._descriptor)
    let url_check = new URL(this._descriptor.urlTemplate)
    if(url_check.origin === "null"){
      return NOT_SETUP //Must return a url to an image or it throws errors so we return a 256x256 blank data uri
    }
    const indexTitle: string = _.get(data, 'indexTitle', '');
    const geoField: string = _.get(data, 'geoField', '');
    const timeFieldName: string = _.get(data, 'timeFieldName', '');
    const dataUrl: string = _.get(data, 'urlTemplate', '');
    const applyGlobalQuery: boolean = _.get(data, 'applyGlobalQuery', true);
    const applyGlobalTime: boolean = _.get(data, 'applyGlobalTime', true);

    if (indexTitle.length === 0) {
      return NOT_SETUP;
    }

    if (geoField.length === 0) {
      return NOT_SETUP;
    }

    if (timeFieldName.length === 0) {
      return NOT_SETUP;
    }

    if (dataUrl.length === 0) {
      return NOT_SETUP;
    }

    let currentParams = "";
    const dataMeta = data
    
    if (dataMeta) {
      const currentParamsObj: any = {};

      if (applyGlobalTime) {
        currentParamsObj.timeFilters = dataMeta.timeFilters;
      }

      currentParamsObj.filters = [];

      if (applyGlobalQuery) {
        const dataMetaFilters = dataMeta.filters || [];
        currentParamsObj.filters = [...dataMetaFilters];
        
        if (dataMeta.query && dataMeta.query.language === "kuery") {
          const kueryNode = fromKueryExpression(dataMeta.query.query);
          const kueryDSL = toElasticsearchQuery(kueryNode);
          currentParamsObj.query = {
            language: "dsl",
            query: kueryDSL,
          };
        } else if (dataMeta.query && dataMeta.query.language === "lucene") {
          const luceneDSL = luceneStringToDsl(dataMeta.query.query);
          currentParamsObj.query = {
            language: "dsl",
            query: luceneDSL,
          };
        } else {
          currentParamsObj.query = dataMeta.query;
        }
      }
      
      currentParamsObj.extent = dataMeta.extent; // .buffer has been expanded to align with tile boundaries
      currentParamsObj.zoom = dataMeta.zoom;
      if (data.query) {
        if (data.query.language === "kuery") {
          const kueryNode = fromKueryExpression(data.query.query);
          const kueryDSL = toElasticsearchQuery(kueryNode);
          currentParamsObj.filters.push({
            "meta": {
              "type" : "bool",
            },
            "query": kueryDSL
          });
        } else if (data.query.language === "lucene") {
          const luceneDSL = luceneStringToDsl(data.query.query);
          currentParamsObj.filters.push({
            "meta": {
              "type" : "bool",
            },
            "query": luceneDSL
           });
        }
      }

      currentParams = currentParams.concat(
        "params=", encodeURIComponent(JSON.stringify(currentParamsObj)),
        "&timestamp_field=", timeFieldName,
        "&geopoint_field=", geoField,
        this.getStyleUrlParams(data),
      );
    }

    const url = dataUrl.concat(
      "/tms/",
      indexTitle,
      "/{z}/{x}/{y}.png?",
      currentParams
    );
    return url
  } catch (error) {
      console.warn(error);
      return NOT_SETUP;
  }
  }
}


const NOT_SETUP = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAQAAAAEACAQAAAD2e2DtAAABu0lEQVR42u3SQREAAAzCsOHf9F6oIJXQS07TxQIABIAAEAACQAAIAAEgAASAABAAAkAACAABIAAEgAAQAAJAAAgAASAABIAAEAACQAAIAAEgAASAABAAAkAACAABIAAEgAAQAAJAAAgAASAABIAAEAACQAAIAAEgAASAABAAAkAACAABIAAEgAAQAAJAAAgAASAABIAAEAACQAAIAAEgAASAABAAAgAACwAQAAJAAAgAASAABIAAEAACQAAIAAEgAASAABAAAkAACAABIAAEgAAQAAJAAAgAASAABIAAEAACQAAIAAEgAASAABAAAkAACAABIAAEgAAQAAJAAAgAASAABIAAEAACQAAIAAEgAASAABAAAkAACAABIAAEgAAQAAJAAAgAASAABIAAEAACQAAIAAAsAEAACAABIAAEgAAQAAJAAAgAASAABIAAEAACQAAIAAEgAASAABAAAkAACAABIAAEgAAQAAJAAAgAASAABIAAEAACQAAIAAEgAASAABAAAkAACAABIAAEgAAQAAJAAAgAASAABIAAEAACQAAIAAEgAASAABAAAkAACAABIAAEgAAQAAJAAKg9kK0BATSHu+YAAAAASUVORK5CYII=" //empty image