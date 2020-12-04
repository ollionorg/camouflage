package com.cldcvr.camouflage.core.mask.types.impl;

import com.cldcvr.camouflage.core.mask.types.AbstractMaskType;

import java.io.Serializable;

/**
 * Replace a regex match string with user string
 */
public class ReplaceConfig extends AbstractMaskType implements Serializable {
    private final String replace;

    /**
     * Accepts a string to replace
     * @param replace
     */
    public ReplaceConfig(String replace) {
        this.replace = replace;
    }

    /**
     * Name of MaskType.
     * @return
     */
    public String name() {
        return "REPLACE_CONFIG";
    }


    /**
     * Apply masking over input
     *
     * @param input String to mask
     * @param regex regex to apply
     * @return
     */
    public String applyMaskStrategy(String input, String regex) {
        if (input == null)
            return null;
        return input.replaceAll(regex, replace);

    }


}
