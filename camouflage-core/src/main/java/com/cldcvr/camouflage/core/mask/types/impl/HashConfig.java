package com.cldcvr.camouflage.core.mask.types.impl;

import com.cldcvr.camouflage.core.mask.types.AbstractMaskType;

import java.io.Serializable;

public final class HashConfig extends AbstractMaskType implements Serializable {

    private final String replace;
    private final String salt;

    public HashConfig(String replace,String salt)
    {
        this.replace=replace;
        this.salt=salt;
    }

    public String name() {
        return "HASH_CONFIG";
    }

    public String applyMaskStrategy(String input, String regex) {
        return input.replace(regex,input.hashCode()+salt);
    }
}
