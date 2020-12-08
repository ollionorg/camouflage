package com.cldcvr.camouflage.core.mask.types.impl;

import org.junit.Test;

public class CharacterMaskConfigTest {

    @Test
    public void testCharacterConfig()
    {
        CharacterMaskConfig characterMaskConfig = new CharacterMaskConfig('#');
        characterMaskConfig.applyMaskStrategy("$115","");
    }
}
