package com.cldcvr.camouflage.core.mask.types.impl;

import com.cldcvr.camouflage.core.json.serde.RangeToValue;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

public class RangeConfigTest {
    static RangeConfig rangeConfig = null;

    static {
        try {
            rangeConfig = new RangeConfig(Arrays.asList(
                    new RangeToValue("1", "30", "LOW"),
                    new RangeToValue("31", "60", "MID"),
                    new RangeToValue("61", "100", "HIGH")), "STANDARD");
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Failed to construct RangeConfig");
        }
    }

    @Test
    public void testWithUnorderedRanges() throws Exception {
        RangeConfig rangeConfigUnordered = new RangeConfig(Arrays.asList(
                new RangeToValue("61", "100", "HIGH"),
                new RangeToValue("31", "60", "MID"),
                new RangeToValue("1", "30", "LOW")), "STANDARD");
        testRanges(rangeConfigUnordered);
    }

    @Test
    public void testRangeConfig() {
        //test ranges
        testRanges(rangeConfig);
        //test outside range
        test( "101", "STANDARD");
        //test random string
        test("asdasda", "STANDARD");
        //test empty string
        test("", "STANDARD");
        //test null input
        test(null, "STANDARD");
    }

    private void testRanges(RangeConfig rangeConfigTest) {
        IntStream.range(1, 30).mapToObj(i -> i + "").forEach(input -> {
            test(rangeConfigTest,input, "LOW");
        });
        IntStream.range(31, 60).mapToObj(i -> i + "").forEach(input -> {
            test(rangeConfigTest,input, "MID");
        });
        IntStream.range(61, 100).mapToObj(i -> i + "").forEach(input -> {
            test(rangeConfigTest,input, "HIGH");
        });
    }

    @Test
    public void testWithLongValue()
    {
        //Test with long value
        RangeConfig config =null;
        try{
            config= new RangeConfig(Arrays.asList(new RangeToValue("9883487623", "9883487800", "HIGHEST"),
                    new RangeToValue("9883487801", "9883488800", "EXCEEDS LIMITS")), "STANDARD");
        }
        catch (Exception e)
        {
            Assert.fail("Should not have failed");
        }
        test(config,"9883487723","HIGHEST");
        test(config,"9883488710","EXCEEDS LIMITS");
        test(config,"988348781000","STANDARD");
    }

    @Test
    public void testWithDoubleValue()
    {
        //Test with long value
        RangeConfig config =null;
        try{
            config= new RangeConfig(Arrays.asList(new RangeToValue("9883487623.83463", "9883487800.87343", "HIGHEST"),
                    new RangeToValue("9883487801.824623", "9883488800.64532", "EXCEEDS LIMITS")), "STANDARD");
        }
        catch (Exception e)
        {
            Assert.fail("Should not have failed");
        }
        test(config,"9883487723.98828732","HIGHEST");
        test(config,"9883488100.553","EXCEEDS LIMITS");
        test(config,"9883488100.55383592742","EXCEEDS LIMITS");
        test(config,"988348781000.89076544","STANDARD");
    }
    @Test
    public void testExceptionWhenCreatingObjectWithIncorrectParameters() throws Exception {
        String nullCheckMessage = "Either min or max or value is null or empty for";
        //Max is empty
        testObjectFailure(Arrays.asList(new RangeToValue("1", null, "LOW")),
                "The object creation should have failed because max is empty",
                nullCheckMessage);
        //Min is empty
        testObjectFailure(Arrays.asList(new RangeToValue(null, "10", "LOW")),
                "The object creation should have failed because min is empty",
                nullCheckMessage);

        //Value is empty
        testObjectFailure(Arrays.asList(new RangeToValue("1", "10", null)),
                "The object creation should have failed because min is null",
                nullCheckMessage);
        //Two range of same values
        testObjectFailure(Arrays.asList(new RangeToValue("1", "10", "LOW"),
                new RangeToValue("11", "20", "LOW")),
                "The object creation should have failed because we have duplicate values",
                "Multiple ranges cannot have the same replacement value");
        //Same ranges
        testObjectFailure(Arrays.asList(new RangeToValue("1", "20", "LOW"),
                new RangeToValue("18", "25", "MID")),
                "The object creation should have failed because we have duplicate ranges",
                "Entry 1.0=Range{upper=20.0, value='LOW'} already buckets BucketProperties{min='18', max='25', value='MID'}");

        //Min is not numeric
        testObjectFailure(Arrays.asList(new RangeToValue("afsd", "10", "LOW")),
                "The object creation should have failed because min cannot be parsed to Double",
                //Assert number format exception
                "For input string: \"afsd\"");

        //Min is greater than max
        testObjectFailure(Arrays.asList(new RangeToValue("15", "11", "LOW")),
                "The object creation should have failed because min is greater than max",
                //Assert number format exception
                "Max [11.0] should be greater than Min [15.0]");

    }

    public void testObjectFailure(List<RangeToValue> range, String failureMessage, String successMessage) {
        try {
            System.out.println("Test :" + failureMessage);
            RangeConfig rangeConfig = new RangeConfig(range, "STANDARD");
            Assert.fail(failureMessage);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(e.getMessage().startsWith(successMessage));
        }
    }

    private void test(RangeConfig rangeConfig, String input, String expected) {
        try {
            String result = rangeConfig.applyMaskStrategy(input, "");
            Assert.assertEquals(String.format("Input [%s] should return [%s]", input, expected), expected, result);
        } catch (Exception e) {
            Assert.fail("Exception was thrown when processing " + e.getMessage());
        }

    }

    private void test(String input, String expected) {
        test(rangeConfig, input, expected);
    }

}
