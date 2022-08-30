import React, { Component, ChangeEvent } from 'react';
import { EuiFormRow, EuiFieldText } from '@elastic/eui';

interface Props {
    value: string;
    valid: boolean;
    onChange: (evt: ChangeEvent<HTMLInputElement>) => void;
};

interface State {};

export class DatashaderUrlEditorField extends Component<Props, State> {
    render() {
        return (
          <EuiFormRow label="Url">
            <EuiFieldText
              placeholder={'https://a.datashader.com'}
              value={this.props.value}
              onChange={this.props.onChange}
              isInvalid={!this.props.valid}
            />
          </EuiFormRow>
        );
    };
}