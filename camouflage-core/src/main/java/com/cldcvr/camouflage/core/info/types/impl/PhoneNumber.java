package com.cldcvr.camouflage.core.info.types.impl;

import com.cldcvr.camouflage.core.info.types.AbstractInfoType;
import com.cldcvr.camouflage.core.mask.types.AbstractMaskType;

/**
 * PhoneNumber infotyoe is used for masking phone number found in DLP scan
 */
public final class PhoneNumber extends AbstractInfoType {

    private final AbstractMaskType maskType;

    /**
     * Accepts a masking type which expresses the masking strategy that can be used here.
     *
     * @param maskType
     */
    public PhoneNumber(AbstractMaskType maskType) {
        this.maskType = maskType;
    }

    /**
     * Name of the infotype
     *
     * @return
     */
    public String name() {
        return "PHONE_NUMBER";
    }


    /**
     * Returns maskType used by the object
     *
     * @return
     */
    public AbstractMaskType getMaskStrategy() {
        return this.maskType;
    }

    /**
     * Regex expresses what will be masked.
     *
     * @return
     */
    public String regex() {
        return ".";
    }

    /**
     * Applies the masking strategy over input data
     *
     * @param input Data to mask
     * @return masked data using the maskType accepted by object.
     */
    public String algorithm(String input) {
        return maskType.applyMaskStrategy(input, regex());
    }
}
