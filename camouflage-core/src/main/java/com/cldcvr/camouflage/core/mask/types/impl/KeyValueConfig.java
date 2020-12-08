package com.cldcvr.camouflage.core.mask.types.impl;

import com.cldcvr.camouflage.core.json.serde.KeyToValue;
import com.cldcvr.camouflage.core.mask.types.AbstractMaskType;
import com.cldcvr.camouflage.core.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class KeyValueConfig extends AbstractMaskType {
    private final Map<String, String> valueMap;
    private static final Logger LOG = LoggerFactory.getLogger(KeyValueConfig.class);

    public KeyValueConfig(List<KeyToValue> itemBuckets) {
        if (itemBuckets == null || itemBuckets.size() == 0) {
            throw new IllegalArgumentException("keyMap list cannot be null or empty when using " + name());
        }
        valueMap = itemBuckets.stream()
                .filter(kv -> Utils.isNotNullOrEmpty(kv.getItem(),
                        () -> new IllegalArgumentException(String.format("Keys cannot be null or empty %s", kv))))
                .collect(Collectors.toMap(k -> k.getItem().toUpperCase(), KeyToValue::getReplacement));
        System.out.println("Key Value mapping constructed " + valueMap);
    }

    @Override
    public String name() {
        return "KEY_VALUE_CONFIG";
    }

    @Override
    public String applyMaskStrategy(String input, String regex) {
        return Utils.isNotNullOrEmpty(input) ? valueMap.getOrDefault(input.toUpperCase(), input) : input;
    }
}
