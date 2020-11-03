package com.cldcvr.camouflage.core.mask.types;

public abstract class AbstractMaskType {
    public abstract String name();
    public abstract String applyMaskStrategy(String input,String regex);
}
