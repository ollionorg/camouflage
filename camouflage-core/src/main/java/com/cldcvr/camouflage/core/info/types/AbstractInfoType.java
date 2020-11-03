package com.cldcvr.camouflage.core.info.types;

import com.cldcvr.camouflage.core.mask.types.AbstractMaskType;

public abstract class AbstractInfoType {
     public abstract String name();

    public abstract AbstractMaskType getMaskStrategy();

    public abstract String regex();

    public abstract String algorithm(String input);
}
