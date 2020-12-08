package com.cldcvr.camouflage.core.util;

import java.util.function.Supplier;

public class Utils {

    public static boolean isNotNullOrEmpty(String val) {
        return !isNull(val) && !val.equals("");
    }

    public static boolean isNotNullOrEmpty(String val, Supplier<Throwable> supplier) {
        if (!isNull(val) && !val.equals("")) {
            return true;
        } else {
            throw new RuntimeException(supplier.get());
        }
    }

    public static boolean isNull(String val) {
        return val == null;
    }
}
