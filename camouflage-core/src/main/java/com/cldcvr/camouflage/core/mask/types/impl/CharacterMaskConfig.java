package com.cldcvr.camouflage.core.mask.types.impl;

import com.cldcvr.camouflage.core.mask.types.AbstractMaskType;
import java.io.Serializable;

/**
 * Replace alphabets in input string with replacement character
 */
public class CharacterMaskConfig extends AbstractMaskType implements Serializable {

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
