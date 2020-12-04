package com.cldcvr.camouflage.core.util;

import com.cldcvr.camouflage.core.exception.CamouflageApiException;
import com.cldcvr.camouflage.core.info.types.AbstractInfoType;
import com.cldcvr.camouflage.core.info.types.impl.Email;
import com.cldcvr.camouflage.core.info.types.impl.Generic;
import com.cldcvr.camouflage.core.info.types.impl.PhoneNumber;
import com.cldcvr.camouflage.core.json.serde.TypeMetadata;
import com.cldcvr.camouflage.core.mask.types.AbstractMaskType;
import com.cldcvr.camouflage.core.mask.types.impl.CharacterMaskConfig;
import com.cldcvr.camouflage.core.mask.types.impl.HashConfig;
import com.cldcvr.camouflage.core.mask.types.impl.RedactConfig;
import com.cldcvr.camouflage.core.mask.types.impl.ReplaceConfig;

import java.util.HashMap;
import java.util.Map;

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
        switch (metadata.getInfoType()) {
            case "PHONE_NUMBER":
                return new PhoneNumber(getMaskTypes(metadata));
            case "EMAIL":
                return new Email(getMaskTypes(metadata));
            default:
                return new Generic(getMaskTypes(metadata));
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
        switch (metadata.getMaskType()) {
            case "REPLACE_CONFIG":
                return new ReplaceConfig(metadata.getReplace());
            case "REDACT_CONFIG":
                return new RedactConfig(metadata.getReplace());
            case "HASH_CONFIG":
                return new HashConfig(metadata.getSalt());
            case "CHARACTER_MASK_CONFIG":
                return new CharacterMaskConfig(metadata.getReplace().charAt(0));
            default:
                throw new CamouflageApiException(String.format("Mask Type `%s` not supported", metadata.getMaskType()));
        }
    }

}
