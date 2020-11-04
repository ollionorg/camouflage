package com.cldcvr.camouflage.core.mask.types.impl;

import com.cldcvr.camouflage.core.mask.types.AbstractMaskType;
import java.io.Serializable;

public class ReplaceConfig extends AbstractMaskType implements Serializable {
    private final String replace;
    public ReplaceConfig(String replace)
    {
        this.replace=replace;
    }

    public String name() {
        return "REPLACE_CONFIG";
    }

    public String applyMaskStrategy(String input, String regex) {

        return input.replaceAll(regex,replace);

    }


}
