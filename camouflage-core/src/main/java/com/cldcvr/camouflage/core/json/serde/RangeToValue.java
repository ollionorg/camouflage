package com.cldcvr.camouflage.core.json.serde;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Comparator;
import java.util.DuplicateFormatFlagsException;
import java.util.Objects;

public class RangeToValue {
    private final String min;
    private final String max;
    private final String value;


    public RangeToValue(@JsonProperty("min") String min, @JsonProperty("max") String max, @JsonProperty("replacement") String value) {
        this.min = min;
        this.max = max;
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public String getMax() {
        return max;
    }

    public String getMin() {
        return min;
    }

    public static RangeToValueComparator getComparator() {
        return new RangeToValueComparator();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RangeToValue that = (RangeToValue) o;
        return Objects.equals(min, that.min) &&
                Objects.equals(max, that.max) &&
                Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(min, max, value);
    }

    @Override
    public String toString() {
        return "BucketProperties{" +
                "min='" + min + '\'' +
                ", max='" + max + '\'' +
                ", value='" + value + '\'' +
                '}';
    }

    private static class RangeToValueComparator implements Comparator<RangeToValue> {
        @Override
        public int compare(RangeToValue v1, RangeToValue v2) {
            Double v1Min = Double.parseDouble(v1.getMin());
            Double v2Min = Double.parseDouble(v2.getMin());

            if (v1Min > v2Min) {
                return 1;
            } else if (v1Min < v2Min) {
                return -1;
            } else {
                return 0;
            }
        }
    }
}

