package com.cldcvr.camouflage.core.mask.types.impl;

import com.cldcvr.camouflage.core.json.serde.KeyToValue;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class KeyValueConfigTest {

    static KeyValueConfig keyValueConfig = null;

    static {
        try {
            keyValueConfig = new KeyValueConfig(Arrays.asList(
                    new KeyToValue("Tokyo Stock Exchange", "TSE"),
                    new KeyToValue("Shanghai Stock Exchange", "SSE"),
                    new KeyToValue("CME Mercantile Exchange", "CME"),
                    new KeyToValue("New York Stock Exchange", "NYE"),
                    //User wants to annotate them as BANK-{}
                    new KeyToValue("JPMorgan Chase & Co", "BANK-1"),
                    new KeyToValue("Bank of America Corp", "BANK-2")));
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Basic KeyValueConfig failure.");
        }
    }

    @Test
    public void testKeyToValueConfig() {
        //Test basic with all right inputs
        test(keyValueConfig, "Tokyo Stock Exchange", "TSE");
        test(keyValueConfig, "Shanghai Stock Exchange", "SSE");
        test(keyValueConfig, "CME Mercantile Exchange", "CME");
        test(keyValueConfig, "New York Stock Exchange", "NYE");
        test(keyValueConfig, "JPMorgan Chase & Co", "BANK-1");
        test(keyValueConfig, "Bank of America Corp", "BANK-2");

        //Test with input case change. Test should succeed because replacement is case insensitive.
        test(keyValueConfig, "Tokyo Stock Exchange".toUpperCase(), "TSE");
        test(keyValueConfig, "Shanghai Stock Exchange".toUpperCase(), "SSE");
        //Lower case
        test(keyValueConfig, "Tokyo Stock Exchange".toLowerCase(), "TSE");
        test(keyValueConfig, "Shanghai Stock Exchange".toLowerCase(), "SSE");
        //Random Case
        test(keyValueConfig, "TOkYo STOCK ExchAngE", "TSE");
        test(keyValueConfig, "sHanGHAi stock ExchangE", "SSE");
    }

    @Test
    public void testNegativeScenarios() {
        //Test with null list
        testObjectFailure(null, "Null list of KeyToValue should fail",
                "keyMap list cannot be null or empty when using KEY_VALUE_CONFIG");
        //Test with empty KeyToValue list
        testObjectFailure(Arrays.asList(), "Empty list of KeyToValue should fail",
                "keyMap list cannot be null or empty when using KEY_VALUE_CONFIG");
        //Test with null key
        testObjectFailure(Arrays.asList(
                new KeyToValue("Tom", "Tom Hanks"),
                new KeyToValue(null, ""),
                new KeyToValue("John", "John Smith")),
                "Some kv is empty so it should throw an exception",
                "");
        //Test with 2 same keys
        testObjectFailure(Arrays.asList(
                new KeyToValue("Tom", "Tom Hanks"),
                new KeyToValue("Tom", "Tom Hanks")),
                "Repetitive keys should throw an exception", "");
        //Test with 2 same keys different case
        testObjectFailure(Arrays.asList(
                new KeyToValue("Tom", "Tom Hanks"),
                new KeyToValue("TOM", "Tom Hanks")),
                "Repetitive keys with case change should throw an exception", "");
        //Test with 2 same keys different values
        testObjectFailure(Arrays.asList(
                new KeyToValue("Tom", "Tommy Hanky"),
                new KeyToValue("TOM", "Tom Hanks 007")),
                "Repetitive keys with case change and different values", "");
    }

    public void testObjectFailure(List<KeyToValue> kv, String failureMessage, String successMessage) {
        try {
            System.out.println("Test :" + failureMessage);
            KeyValueConfig keyValueConfigTest = new KeyValueConfig(kv);
            Assert.fail(failureMessage);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(e.getMessage().startsWith(successMessage));
        }
    }

    @Test
    public void flaky()
    {

    }

    private void test(KeyValueConfig keyValueConfig, String input, String expected) {
        try {
            System.out.println(String.format("Test with Input: [%s] expected return [%s]", input, expected));
            String result = keyValueConfig.applyMaskStrategy(input, "");
            Assert.assertEquals(String.format("Input [%s] should return [%s]", input, expected), expected, result);
        } catch (Exception e) {
            Assert.fail("Exception was thrown when processing " + e.getMessage());
        }

    }
}
