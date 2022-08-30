import type { CoreStart } from '@kbn/core/public';

import type { MapsCustomRasterSourcePluginStart } from './types';

let coreStart: CoreStart;
let pluginsStart: MapsCustomRasterSourcePluginStart;

export function setStartServices(core: CoreStart, plugins: MapsCustomRasterSourcePluginStart) {
  coreStart = core;
  pluginsStart = plugins;
}
export const getIndexPatternService = () => pluginsStart.data.dataViews;
export const getToasts = () => coreStart.notifications.toasts;
export const getHttp = () => coreStart.http;
export const getIndexPatternSelectComponent = () =>
  pluginsStart.unifiedSearch.ui.IndexPatternSelect;