/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

import { CoreSetup, CoreStart, Plugin } from '@kbn/core/public';
import { MapsCustomRasterSourcePluginSetup, MapsCustomRasterSourcePluginStart } from './types';
import { CustomRasterSource } from './classes/custom_raster_source';
import { customRasterLayerWizard } from './classes/custom_raster_layer_wizard';
import { PLUGIN_ID, PLUGIN_NAME } from '../common';
import { setStartServices } from './kibana_services';


export class MapsCustomRasterSourcePlugin
  implements
    Plugin<void, void, MapsCustomRasterSourcePluginSetup, MapsCustomRasterSourcePluginStart>
{
  public setup(
    core: CoreSetup<MapsCustomRasterSourcePluginStart>,
    {  maps: mapsSetup }: MapsCustomRasterSourcePluginSetup
  ) {
    // Register the Custom raster layer wizard with the Maps application
    mapsSetup.registerSource({
      type: CustomRasterSource.type,
      ConstructorFunction: CustomRasterSource,
    });
    mapsSetup.registerLayerWizard(customRasterLayerWizard);

    // Register an application into the side navigation menu
    core.application.register({
      id: PLUGIN_ID,
      title: PLUGIN_NAME,
      mount: ({ history }) => {
        (async () => {
          const [coreStart] = await core.getStartServices();
          // if it's a regular navigation, open a new map
          if (history.action === 'PUSH') {
            coreStart.application.navigateToApp('maps', { path: 'map' });
          } else {
            coreStart.application.navigateToApp('developerExamples');
          }
        })();
        return () => {};
      },
    });


  }


  public start(core: CoreStart, plugins: MapsCustomRasterSourcePluginStart): void {
    setStartServices(core, plugins);
  }
  public stop() {}
}
