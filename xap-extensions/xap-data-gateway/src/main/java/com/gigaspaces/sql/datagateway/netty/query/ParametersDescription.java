package com.gigaspaces.sql.datagateway.netty.query;

import java.util.Collections;
import java.util.List;

import static java.util.Collections.unmodifiableList;

public class ParametersDescription {
    public static ParametersDescription EMPTY = new ParametersDescription(Collections.emptyList());

    private final List<ParameterDescription> parameters;

    public ParametersDescription(List<ParameterDescription> parameters) {
        this.parameters = parameters;
    }

    public int getParametersCount() {
        return parameters.size();
    }
    public List<ParameterDescription> getParameters() {
        return unmodifiableList(parameters);
    }
}
