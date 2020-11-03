package com.cldcvr.camouflage.core.util;

import com.cldcvr.camouflage.core.info.types.AbstractInfoType;

import java.io.Serializable;
import java.util.Objects;
import java.util.Set;

public final class CamouflageTraits implements Serializable {
    private final String column;
    private final Set<AbstractInfoType> types;

    public CamouflageTraits(String column, Set<AbstractInfoType> types)
    {
        this.column=column;
        this.types=types;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CamouflageTraits that = (CamouflageTraits) o;
        return Objects.equals(column, that.column) &&
                Objects.equals(types, that.types);
    }

    @Override
    public int hashCode() {
        return Objects.hash(column, types);
    }

    public String getColumn() {
        return column;
    }

    public Set<AbstractInfoType> getTypes() {
        return types;
    }

    @Override
    public String toString() {
        return "CamouflageTraits{" +
                "column='" + column + '\'' +
                ", types=" + types +
                '}';
    }
}
