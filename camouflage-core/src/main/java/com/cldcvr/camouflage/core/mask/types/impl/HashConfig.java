package com.cldcvr.camouflage.core.mask.types.impl;

import com.cldcvr.camouflage.core.mask.types.AbstractMaskType;

public final class HashConfig extends AbstractMaskType {

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
