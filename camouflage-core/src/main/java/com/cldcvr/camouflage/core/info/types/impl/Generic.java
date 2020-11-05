package com.cldcvr.camouflage.core.info.types.impl;

import com.cldcvr.camouflage.core.info.types.AbstractInfoType;
import com.cldcvr.camouflage.core.mask.types.AbstractMaskType;

public class Generic extends AbstractInfoType {

    private final AbstractMaskType maskType;

    public Generic(AbstractMaskType maskType) {
        this.maskType = maskType;
    }

    public String name() {
        return "GENERIC";
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
