package com.cldcvr.camouflage.core.json.serde;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Objects;

public class KeyToValue implements Serializable {

    private final String item;
    private final String replacement;

    public KeyToValue(@JsonProperty("item") String item, @JsonProperty("replacement") String replacement) {
        this.item = item;
        this.replacement = replacement;
    }

    public String getItem() {
        return item;
    }

    public String getReplacement() {
        return replacement;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KeyToValue that = (KeyToValue) o;
        return Objects.equals(item, that.item) &&
                Objects.equals(replacement, that.replacement);
    }

    @Override
    public int hashCode() {
        return Objects.hash(item, replacement);
    }

    @Override
    public String toString() {
        return "KeyToValue{" +
                "item='" + item + '\'' +
                ", replacement='" + replacement + '\'' +
                '}';
    }
}
