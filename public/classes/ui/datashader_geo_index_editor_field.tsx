import React, { Component } from 'react';
import { GeoIndexPatternSelect } from './geo_index_pattern_select';

import { DataView } from '@kbn/data-plugin/common';
interface Props {
    value: string;
    onChange: (indexPattern: DataView) => void;
};

interface State {};

export class DatashaderGeoIndexEditorField extends Component<Props, State> {
    render() {
        return (
          <GeoIndexPatternSelect
            value={this.props.value}
            onChange={this.props.onChange}
          />
        );
    };
}