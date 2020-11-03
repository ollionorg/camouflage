package com.cldcvr.camouflage.core.json.serde;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

public class TypeMetadata implements Serializable {

    private final String infoType;
    private final String maskType;
    private final String replace;
    private final String salt;

    public TypeMetadata(@JsonProperty("info_type") String infoType,
                        @JsonProperty("mask_type") String maskType,
                        @JsonProperty("replace") String replace,
                        @JsonProperty("salt") String salt) {
        this.infoType = infoType;
        this.maskType = maskType;
        this.replace = replace;
        this.salt = salt;
    }

    public String getSalt() {
        return salt;
    }

    public String getReplace() {
        return replace;
    }

    public String getMaskType() {
        return maskType;
    }

    public String getInfoType() {
        return infoType;
    }


    @Override
    public String toString() {
        return "com.cldcvr.camouflage.core.json.serde.TypeMetadata{" +
                "infoType='" + infoType + '\'' +
                ", maskType='" + maskType + '\'' +
                ", replace='" + replace + '\'' +
                ", salt='" + salt + '\'' +
                '}';
    }

}
