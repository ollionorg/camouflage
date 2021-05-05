package com.cldcvr.camouflage.core.util;

import com.cldcvr.camouflage.core.exception.CamouflageApiException;
import com.cldcvr.camouflage.core.info.types.AbstractInfoType;
import com.cldcvr.camouflage.core.info.types.impl.Email;
import com.cldcvr.camouflage.core.info.types.impl.Generic;
import com.cldcvr.camouflage.core.info.types.impl.PhoneNumber;
import com.cldcvr.camouflage.core.json.serde.TypeMetadata;
import com.cldcvr.camouflage.core.mask.types.AbstractMaskType;
import com.cldcvr.camouflage.core.mask.types.impl.*;
import com.cldcvr.camouflage.core.mask.types.impl.KeyValueConfig;
import com.cldcvr.camouflage.core.mask.types.impl.RangeConfig;

/**
 * Class is used create infoTypes and maskTypes based on string representation
 */
public class FetchInfoAndMaskTypes {

    /**
     * Accept {@link TypeMetadata} and returns {@link AbstractInfoType}
     *
     * @param metadata Masking and InfoType information for objects to be created
     * @return AbstractInfoType or then {@link Generic} if no match
     * @throws Exception
     */
    public static AbstractInfoType getInfoType(TypeMetadata metadata) throws Exception {
        if (Utils.isNotNullOrEmpty(metadata.getInfoType())) {
            switch (metadata.getInfoType().toUpperCase()) {
                case "PHONE_NUMBER":
                    return new PhoneNumber(getMaskTypes(metadata));
                case "EMAIL":
                    return new Email(getMaskTypes(metadata));
                default:
                    return new Generic(getMaskTypes(metadata));
            }
        } else {
            throw new IllegalArgumentException("InfoType name cannot be null or empty");
        }
    }

    /**
     * Accepts {@link TypeMetadata} and gives back {@link AbstractMaskType}
     *
     * @param metadata TypeMetadata used to create {@link AbstractMaskType}
     * @return AbstractMaskType or exception if no match found.
     * @throws Exception
     */
    public static AbstractMaskType getMaskTypes(TypeMetadata metadata) throws Exception {
        if (Utils.isNotNullOrEmpty(metadata.getMaskType())) {
            switch (metadata.getMaskType().toUpperCase()) {
                case "REPLACE_CONFIG":
                    return new ReplaceConfig(metadata.getReplace());
                case "REDACT_CONFIG":
                    return new RedactConfig(metadata.getReplace());
                case "HASH_CONFIG":
                    return new HashConfig(metadata.getSalt());
                case "CHARACTER_MASK_CONFIG":
                    return new CharacterMaskConfig(metadata.getReplace().charAt(0));
                case "RANGE_CONFIG":
                    return new RangeConfig(metadata.getBuckets(), metadata.getReplace());
                case "KEY_VALUE_CONFIG":
                    return new KeyValueConfig(metadata.getKvList());
                default:
                    throw new CamouflageApiException(String.format("Mask Type `%s` not supported", metadata.getMaskType()));
            }
        } else {
            throw new IllegalArgumentException("MaskType name cannot be null or empty");
        }
    }


}
