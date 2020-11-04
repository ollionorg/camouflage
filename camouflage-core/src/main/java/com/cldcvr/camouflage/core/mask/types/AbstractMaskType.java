package com.cldcvr.camouflage.core.mask.types;

import java.io.Serializable;

public abstract class AbstractMaskType implements Serializable {
    public abstract String name();
    public abstract String applyMaskStrategy(String input,String regex);
}
