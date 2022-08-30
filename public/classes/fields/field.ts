/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

import type {
  AggregationsExtendedStatsAggregation,
  AggregationsPercentilesAggregation,
  AggregationsTermsAggregation,
} from '@elastic/elasticsearch/lib/api/typesWithBodyKey';
import { TileMetaFeature } from '@kbn/maps-plugin//common/descriptor_types';

import { FIELD_ORIGIN, } from '@kbn/maps-plugin/common';
import { ISource } from '@kbn/maps-plugin/public/classes/sources/source';

export interface IField {
  getName(): string;
}

export class AbstractField implements IField {
  private readonly _fieldName: string;
  private readonly _origin: FIELD_ORIGIN;

  constructor({ fieldName, origin,source }: { fieldName: string; origin: FIELD_ORIGIN; source: ISource; }) {
    this._fieldName = fieldName;
    this._origin = origin;
  }

  supportsFieldMetaFromEs(): boolean {
    throw new Error('must implement AbstractField#supportsFieldMetaFromEs');
  }

  supportsFieldMetaFromLocalData(): boolean {
    throw new Error('must implement AbstractField#supportsFieldMetaFromLocalData');
  }

  getName(): string {
    return this._fieldName;
  }

  getMbFieldName(): string {
    return this.getName();
  }

  getRootName(): string {
    return this.getName();
  }

  canValueBeFormatted(): boolean {
    return false;
  }

  getSource(): ISource {
    throw new Error('must implement AbstractField#getSource');
  }

  isValid(): boolean {
    return !!this._fieldName;
  }

  async getDataType(): Promise<string> {
    return 'string';
  }

  async getLabel(): Promise<string> {
    return this._fieldName;
  }


  getOrigin(): FIELD_ORIGIN {
    return this._origin;
  }

  async getExtendedStatsFieldMetaRequest(): Promise<Record<
    string,
    { extended_stats: AggregationsExtendedStatsAggregation }
  > | null> {
    return null;
  }

  async getPercentilesFieldMetaRequest(
    percentiles: number[]
  ): Promise<Record<string, { percentiles: AggregationsPercentilesAggregation }> | null> {
    return null;
  }

  async getCategoricalFieldMetaRequest(
    size: number
  ): Promise<Record<string, { terms: AggregationsTermsAggregation }> | null> {
    return null;
  }

  isEqual(field: IField) {
    return this._origin === field.getOrigin() && this._fieldName === field.getName();
  }

  pluckRangeFromTileMetaFeature(metaFeature: TileMetaFeature) {
    return null;
  }

  isCount() {
    return false;
  }
}
