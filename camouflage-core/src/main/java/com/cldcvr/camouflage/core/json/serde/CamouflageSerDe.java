package com.cldcvr.camouflage.core.json.serde;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.List;

public class CamouflageSerDe implements Serializable {
    private final List<ColumnMetadata> dlpMetadata;

    public CamouflageSerDe(@JsonProperty("DLPMetadata") List<ColumnMetadata> dlpMetadata) {
        this.dlpMetadata = dlpMetadata;
    }

    public List<ColumnMetadata> getDlpMetadata() {
        return dlpMetadata;
    }

    @Override
    public String toString() {
        return "com.cldcvr.json.serde.JsonSerDe{" +
                "dlpMetadata=" + dlpMetadata +
                '}';
    }

    /**
     * {
     *     DLPMetadata:{
     *
     *     }
     * }
     */
}
