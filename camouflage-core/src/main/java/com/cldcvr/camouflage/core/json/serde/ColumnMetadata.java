package com.cldcvr.camouflage.core.json.serde;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.List;

/**
 * Class is used for jackson parsing from JSON string to Object representation
 */
public class ColumnMetadata implements Serializable {

    private final String column;
    private final List<TypeMetadata> dlpTypes;

    /**
     * Accepts a string column name and a List of {@link TypeMetadata}. {@link TypeMetadata} is used to hold information
     * about InfoTypes and MaskTypes that will be applied on a given column.
     *
     * @param column Column name on which masking will do done.
     * @param dlpTypes List of masking infoTypes and maskTypes to be used.
     */
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
