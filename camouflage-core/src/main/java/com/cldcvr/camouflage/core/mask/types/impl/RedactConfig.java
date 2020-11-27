package com.cldcvr.camouflage.core.mask.types.impl;

import com.cldcvr.camouflage.core.mask.types.AbstractMaskType;
import java.io.Serializable;

public class RedactConfig extends AbstractMaskType implements Serializable {

    private final String replace;
    public RedactConfig(String replace)
    {
        if (replace == null || replace == "")
            this.replace = "*";
        else if (replace.length() > 1)
            this.replace = replace.substring(0,1);
        else
            this.replace = replace;
    }
    public String name() {
        return "REDACT_CONFIG";
    }

    public String applyMaskStrategy(String input, String regex) {
        if (input == null)
            return null;
        return input.replaceAll(regex,replace);
    }


}
