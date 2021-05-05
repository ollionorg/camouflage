package com.cldcvr.camouflage.core.mask.types.impl;

import org.junit.Assert;
import org.junit.Test;

public class CharacterMaskConfigTest {

    private final CharacterMaskConfig characterMaskConfig = new CharacterMaskConfig('#');
    private final String defaultInput = "$1,111";
    private final String defaultOutput = "$#,###";

    @Test
    public void testCharacterConfig() {
        //Negative test
        testAndAssert("", "", "Empty string returns empty output");
        testAndAssert(null, "", "Null string returns empty output");

        //Valid test
        testAndAssert("111", "###", "String with no special chars should return ###");

        //Input with special char
        testAndAssert("$111", "$###", "String with special chars");
        testAndAssert(defaultInput, defaultOutput, "String with special chars");
        testAndAssert("Alpha-1", "#####-#", "String with special chars");
        testAndAssert("$inb23ruib23i5_4i--23n@k4k23(b4k2", "$#############_##--###@#####(####",
                "String alphanumeric with special chars");
    }

    @Test
    public void testObjectFailure() {
        try {
            CharacterMaskConfig config = new CharacterMaskConfig(null);
        } catch (Exception e) {
            Assert.fail("Object should have not thrown an exception");
        }
    }

    @Test
    public void testNullMaskingChar() {
        testAndAssert(new CharacterMaskConfig(null),defaultInput,defaultOutput,"");
    }



    private void testAndAssert(String input, String expectedOutput, String message) {
        testAndAssert(characterMaskConfig, input, expectedOutput, message);
    }

    private void testAndAssert(CharacterMaskConfig config, String input, String expectedOutput, String message) {
        String result = config.applyMaskStrategy(input, "");
        Assert.assertEquals(message, result, expectedOutput);
    }
}
