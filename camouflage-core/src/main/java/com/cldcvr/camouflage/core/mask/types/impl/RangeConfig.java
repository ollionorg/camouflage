package com.cldcvr.camouflage.core.mask.types.impl;

import com.cldcvr.camouflage.core.json.serde.RangeToValue;
import com.cldcvr.camouflage.core.mask.types.AbstractMaskType;
import com.cldcvr.camouflage.core.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class RangeConfig extends AbstractMaskType {

    private final static Logger LOG = LoggerFactory.getLogger(RangeConfig.class);

    private final List<RangeToValue> buckets;
    private final RangeBucket config;
    private final String defaultVal;


    public RangeConfig(List<RangeToValue> buckets, String defaultVal) throws Exception {
        if (buckets == null || buckets.size() == 0) {
            throw new IllegalArgumentException("List of buckets cannot be null when using " + name());
        } else {
            this.defaultVal = !Utils.isNull(defaultVal) ? defaultVal : "";
            this.buckets = buckets;
            this.config = parser(buckets);
        }
    }

    @Override
    public String name() {
        return "RANGE_CONFIG";
    }

    @Override
    public String applyMaskStrategy(String input, String regex) {
        return config.apply(input);
    }

    private RangeBucket parser(List<RangeToValue> buckets) throws Exception {
        return new RangeBucket(buckets, defaultVal);
    }

    private static class RangeBucket {
        private static class Range {
            public final Double upper;
            public final String value;

            private Range(Double upper, String value) {
                this.upper = upper;
                this.value = value;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                Range range = (Range) o;
                return Objects.equals(upper, range.upper) &&
                        Objects.equals(value, range.value);
            }

            @Override
            public int hashCode() {
                return Objects.hash(upper, value);
            }

            @Override
            public String toString() {
                return "Range{" +
                        "upper=" + upper +
                        ", value='" + value + '\'' +
                        '}';
            }
        }

        @Override
        public String toString() {
            return "RangeBucket{" +
                    "rangeTreeMap=" + rangeTreeMap +
                    '}';
        }

        private final NavigableMap<Double, Range> rangeTreeMap = new TreeMap<Double, Range>();
        private final String defaultVal;
        private final List<RangeToValue> buckets;
        private final Set<String> valueChecker = new HashSet<>();

        private RangeBucket(List<RangeToValue> buckets, String defaultVal) throws Exception {
            try {
                buckets.sort(RangeToValue.getComparator());
                this.buckets = buckets;
                this.buckets.stream().forEach(r -> rangeCheck(r));
                LOG.info(String.format("RangeBucket has build the following -> %s", rangeTreeMap));
                this.defaultVal = defaultVal;
            } catch (Exception e) {
                LOG.error("RangeBucket failed ", e);
                throw e;
            }
        }

        private void rangeCheck(RangeToValue r) {
            if (Utils.isNotNullOrEmpty(r.getMin()) && Utils.isNotNullOrEmpty(r.getMax()) && Utils.isNotNullOrEmpty(r.getValue())) {
                Double min = Double.parseDouble(r.getMin());
                Double max = Double.parseDouble(r.getMax());
                if (min > max) {
                    throw new IllegalArgumentException(String.format("Max [%s] should be greater than Min [%s] for %s", max, min, r));
                }
                Map.Entry<Double, Range> doubleRangeEntry = rangeTreeMap.floorEntry(min);
                if (doubleRangeEntry != null) {
                    if (doubleRangeEntry.getValue().upper > min) {
                        throw new IllegalArgumentException("Entry " + doubleRangeEntry + " already buckets " + r);
                    }
                }
                if (!valueChecker.contains(r.getValue())) {
                    rangeTreeMap.put(min, new Range(max, r.getValue()));
                    valueChecker.add(r.getValue());
                } else {
                    throw new IllegalArgumentException("Multiple ranges cannot have the same replacement value " + buckets);
                }
            } else {
                throw new IllegalArgumentException(String.format("Either min or max or value is null or empty for [%s]", r));
            }
        }

        public String apply(String input) {
            try {
                double key = Math.ceil(Double.parseDouble(input));
                String output = null;
                Map.Entry<Double, Range> entry = rangeTreeMap.floorEntry(key);
                if (entry == null) {
                    output = defaultVal;
                } else if (key <= entry.getValue().upper) {
                    output = entry.getValue().value;
                } else {
                    output = defaultVal;
                }
                return output;
            } catch (Exception e) {
                LOG.error("RangeBucket failed to apply on value {} returning {}", input, defaultVal, e);
                return defaultVal;
            }
        }
    }

}
