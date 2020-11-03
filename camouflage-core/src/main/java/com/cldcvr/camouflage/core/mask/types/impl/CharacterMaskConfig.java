package com.cldcvr.camouflage.core.mask.types.impl;

import com.cldcvr.camouflage.core.mask.types.AbstractMaskType;

/**
 * Replace alphabets in input string with replacement character
 */
public class CharacterMaskConfig extends AbstractMaskType {

    private final Character ch;
    public CharacterMaskConfig(Character ch)
    {
        this.ch=ch;
    }
    public String name() {
        return "CHARACTER_MASK_CONFIG";
    }

    public String applyMaskStrategy(String input, String regex) {
        return input.replace(regex,null);
    }
}
