/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */


export const GIS_API_PATH = `api/maps`;
export const DEFAULT_MAX_RESULT_WINDOW = 10000;
import { i18n } from '@kbn/i18n';
import { getHttp, getToasts } from '../../../kibana_services';

let toastDisplayed = false;

export async function loadIndexDocCount(indexPatternTitle: string): Promise<number> {
    const fetchPromise = fetchIndexDocCount(indexPatternTitle);
    return fetchPromise;
  }
  
  async function fetchIndexDocCount(indexPatternTitle: string): Promise<number> {
    const http = getHttp();
    const toasts = getToasts();
    try {
      return await http.fetch(`../${GIS_API_PATH}/indexCount`, {
        method: 'GET',
        credentials: 'same-origin',
        query: {
          index: indexPatternTitle,
        },
      });
    } catch (err) {
      const warningMsg = i18n.translate('xpack.maps.indexSettings.fetchErrorMsg', {
        defaultMessage: `Unable to fetch index document count for index pattern '{indexPatternTitle}'`,
        values: {
          indexPatternTitle,
        },
      });
      if (!toastDisplayed) {
        // Only show toast for first failure to avoid flooding user with warnings
        toastDisplayed = true;
        toasts.addWarning(warningMsg);
      }
      // eslint-disable-next-line no-console
      console.warn(warningMsg);
      return DEFAULT_MAX_RESULT_WINDOW;
    }
  }