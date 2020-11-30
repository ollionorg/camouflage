package com.cldcvr.camouflage.core.mask.types.impl;

import com.cldcvr.camouflage.core.info.types.*;
import com.cldcvr.camouflage.core.info.types.impl.*;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;

public class HashConfigTest {

    private final Logger LOG = LoggerFactory.getLogger(HashConfigTest.class);

    @Test
    public void TestHashConfigNullSaltOrNullValue() throws Exception {

        AbstractInfoType infoType = new PhoneNumber(new HashConfig(null));
        String result = infoType.algorithm("hello world");
        LOG.info(result);
        assertEquals("Hash string length should be 64", 64, result.length());

        String result2 = infoType.algorithm(null);
        LOG.info(result2);
        assertEquals("result should be null", null, result2);
    }

    @Test
    public void TestHashConfigEmptySalt() throws Exception {

        AbstractInfoType infoType = new PhoneNumber(new HashConfig(""));
        String result = infoType.algorithm("hello world");
        LOG.info(result);
        assertEquals("Hash string length should be 64", 64, result.length());
    }

    @Test
    public void TestHashConfigForCarrigeReturnChar() throws Exception {

        AbstractInfoType infoType = new PhoneNumber(new HashConfig("12345678901234567890"));
        String result = infoType.algorithm("q23\r");
        assertEquals("Hash string length should be 64", 64, result.length());
    }

    @Test
    public void TestHashConfigLongSaltOrLongInput() throws Exception {

        AbstractInfoType infoType = new PhoneNumber(new HashConfig("12345678901234567890"));
        String result = infoType.algorithm("hello world");
        LOG.info(result);
        assertEquals("Hash string length should be 64", 64, result.length());
    }

    @Test
    public void TestHashConfigLongInput() throws Exception {

        AbstractInfoType infoType = new PhoneNumber(new HashConfig("12345678901234567890"));
        String result = infoType.algorithm("This is a paragraph to test hashing for a very long string. The resultant string should not exceed length of 64 byte no matter how big is input string.");
        LOG.info(result);
        assertEquals("Hash string length should be 64", 64, result.length());

    }

    @Test
    public void TestHashConfigWithNonAsciiChar() throws Exception {
        AbstractInfoType infoType = new PhoneNumber(new HashConfig("12345678901234567890"));
        String result = infoType.algorithm("This is भारत");
        System.out.println(result);
        assertEquals("Hash string length should be 64",64,result.length());

        String result2 = infoType.algorithm("网络");
        System.out.println(result2);
        assertEquals("Hash string length should be 64",64,result2.length());
    }


}
