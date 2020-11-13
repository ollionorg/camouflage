package com.cldcvr.camouflage.spark.relation;

public class Util {
    public static void checkOrThrow(boolean condition, String message) throws IllegalArgumentException {
        if (condition) {
            throw new IllegalArgumentException(message);
        }
    }
}
