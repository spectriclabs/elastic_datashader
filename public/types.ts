/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
import { MapsPluginSetup, MapsPluginStart } from '@kbn/maps-plugin/public/plugin';
import type { DataPublicPluginStart } from '@kbn/data-plugin/public';
import type { UnifiedSearchPublicPluginStart } from '@kbn/unified-search-plugin/public';
export interface MapsCustomRasterSourcePluginSetup {
  maps: MapsPluginSetup;
}
export interface MapsCustomRasterSourcePluginStart {
  maps: MapsPluginStart;
  data: DataPublicPluginStart;
  unifiedSearch: UnifiedSearchPublicPluginStart;
}
