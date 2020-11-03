package com.cldcvr.camouflage.core.json.serde;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.List;

public class ColumnMetadata implements Serializable {

    private final String column;
    private final List<TypeMetadata> dlpTypes;

    public ColumnMetadata(@JsonProperty("column") String column, @JsonProperty("dlpTypes") List<TypeMetadata> dlpTypes) {
        this.column = column;
        this.dlpTypes = dlpTypes;
    }

    public List<TypeMetadata> getDlpTypes() {
        return dlpTypes;
    }

    public String getColumn() {
        return column;
    }


    @Override
    public String toString() {
        return "com.cldcvr.camouflage.core.json.serde.ColumnMetadata{" +
                "column='" + column + '\'' +
                ", dlpTypes=" + dlpTypes +
                '}';
    }
}
