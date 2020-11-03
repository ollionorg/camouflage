package com.cldcvr.camouflage.core.mask.types.impl;

import com.cldcvr.camouflage.core.mask.types.AbstractMaskType;

public class RedactConfig extends AbstractMaskType {

    private final String replace;
    public RedactConfig(String replace)
    {
        this.replace=replace;
    }
    public String name() {
        return "REDACT_CONFIG";
    }

    public String applyMaskStrategy(String input, String regex) {
        return input.replace(regex,replace);
    }


}
