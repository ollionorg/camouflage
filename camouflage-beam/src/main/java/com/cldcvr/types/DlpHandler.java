package com.cldcvr.types;

import com.cldcvr.camouflage.core.info.types.AbstractInfoType;

import java.io.Serializable;
import java.util.List;

public final class DlpHandler implements Serializable {

    private final String column;
    private final List<AbstractInfoType> types;

    public DlpHandler(String column, List<AbstractInfoType> types) {
        this.column = column;
        this.types = types;
    }


    public List<AbstractInfoType> getTypes() {
        return types;
    }

    public String getColumn() {
        return column;
    }

}
