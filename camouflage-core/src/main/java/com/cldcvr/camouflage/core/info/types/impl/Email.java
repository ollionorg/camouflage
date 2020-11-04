package com.cldcvr.camouflage.core.info.types.impl;

import com.cldcvr.camouflage.core.info.types.AbstractInfoType;
import com.cldcvr.camouflage.core.mask.types.AbstractMaskType;

public class Email extends AbstractInfoType {

    private final AbstractMaskType maskType;

    public Email(AbstractMaskType maskType) {
        this.maskType = maskType;
    }

    public String name() {
        return "EMAIL";
    }

    public AbstractMaskType getMaskStrategy() {
        return this.maskType;
    }

    public String regex() {
        return ".";
    }

    public String algorithm(String input) {
        return maskType.applyMaskStrategy(input,regex());
    }
}
