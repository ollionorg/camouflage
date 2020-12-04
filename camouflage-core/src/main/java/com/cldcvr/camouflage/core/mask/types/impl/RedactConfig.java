package com.cldcvr.camouflage.core.mask.types.impl;

import com.cldcvr.camouflage.core.mask.types.AbstractMaskType;

import java.io.Serializable;

/**
 * Redacts the string with given replace char
 */
public class RedactConfig extends AbstractMaskType implements Serializable {

    private final String replace;

    /**
     * Accepts replacements string to replace while masking.
     *
     * @param replace String to use as replacement default is "*"
     */
    public RedactConfig(String replace) {
        if (replace == null || replace == "")
            this.replace = "*";
        else if (replace.length() > 1)
            this.replace = replace.substring(0, 1);
        else
            this.replace = replace;
    }

    /**
     * Name of MaskType
     *
     * @return
     */
    public String name() {
        return "REDACT_CONFIG";
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
