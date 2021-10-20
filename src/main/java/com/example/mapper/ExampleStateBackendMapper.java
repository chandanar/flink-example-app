/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.example.mapper;

import com.example.domain.TextFileObj;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;

/**
 *
 * @author chandanar
 */
public class ExampleStateBackendMapper extends RichMapFunction<TextFileObj, TextFileObj> {

    // ValueState only holds one object for each key - location id
    private transient ValueState<TextFileObj> valueState;
    final String backendName = "location-temp-state";

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<TextFileObj> valueStateDescriptor = new ValueStateDescriptor<TextFileObj>(
                backendName,
                TypeInformation.of(TextFileObj.class)
        );
        valueState = getRuntimeContext().getState(valueStateDescriptor);
    }

    @Override
    public TextFileObj map(TextFileObj newStateValue) throws Exception {
        TextFileObj prevValueState = this.valueState.value();

        if (prevValueState == null) {
            this.valueState.update(newStateValue);
            return newStateValue;
        } else {
            if(prevValueState.getTemperature() >= newStateValue.getTemperature()) {
                return null;
            } else {
                this.valueState.update(newStateValue);
                return newStateValue;
            }            
        }
    }
}
