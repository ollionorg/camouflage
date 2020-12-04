package com.cldcvr.camouflage.core.json.serde;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

/**
 * Class is used for jackson parsing from JSON string to Object representation
 */
public class TypeMetadata implements Serializable {

    private final String infoType;
    private final String maskType;
    private final String replace;
    private final String salt;

    /**
     * Accepts params to express the required InfoType with MaskType to be used with replacement string if at all
     * regex based approach is used (Optional) or accepts a salt string is hash based masking is used (Optional)
     *
     * @param infoType String name of the infoType to be used.
     * @param maskType String name of the MaskType to be used.
     * @param replace  String to replace if a regex match is applied
     * @param salt     String to use as salt if hash based mask is applied
     */
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
