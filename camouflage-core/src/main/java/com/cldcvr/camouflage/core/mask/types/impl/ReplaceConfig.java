package com.cldcvr.camouflage.core.mask.types.impl;

import com.cldcvr.camouflage.core.mask.types.AbstractMaskType;

public class ReplaceConfig extends AbstractMaskType {
    private final String replace;
    public ReplaceConfig(String replace)
    {
        this.replace=replace;
    }

    public String name() {
        return "REPLACE_CONFIG";
    }

    public String applyMaskStrategy(String input, String regex) {
        return input.replace(regex, String.format("[%s]",replace));
    }


}
