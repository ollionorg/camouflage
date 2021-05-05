package com.cldcvr.camouflage.core.mask.types.impl;

import com.cldcvr.camouflage.core.mask.types.AbstractMaskType;

import java.io.Serializable;

/**
 * Replace alphabets in input string with replacement character
 */
public class CharacterMaskConfig extends AbstractMaskType implements Serializable {

    private final Character MASKING_CHAR;

    /**
     * Accepts to be replace
     *
     * @param MASKING_CHAR
     */
    public CharacterMaskConfig(Character MASKING_CHAR) {
        this.MASKING_CHAR = MASKING_CHAR == null ? '#' : MASKING_CHAR;
    }

    public String name() {
        return "CHARACTER_MASK_CONFIG";
    }

    public String applyMaskStrategy(String input, String regex) {
        if (input == null || input.trim().equals("")) {
            return "";
        }
        StringBuffer returnBuffer = new StringBuffer();
        input.chars().mapToObj(i -> (char) i).forEach(character ->
                returnBuffer.append(Character.isAlphabetic(character) || Character.isDigit(character) ? MASKING_CHAR : character));
        return returnBuffer.toString();
    }
}
