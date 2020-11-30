package com.cldcvr.camouflage.core.mask.types.impl;

import com.cldcvr.camouflage.core.info.types.AbstractInfoType;
import com.cldcvr.camouflage.core.info.types.impl.PhoneNumber;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;

public class RedactConfigTest {

    private final Logger LOG = LoggerFactory.getLogger(RedactConfigTest.class);

    @Test
    public void TestRedactConfig() throws Exception {

        AbstractInfoType infoType = new PhoneNumber(new RedactConfig("#"));
        String result = infoType.algorithm("hello world");
        LOG.info(result);
        assertEquals("input should be replaced with #", "###########", result);
    }

    @Test
    public void TestRedactConfigForCarrigeReturnChar() throws Exception {

        AbstractInfoType infoType = new PhoneNumber(new RedactConfig("#"));
        String result = infoType.algorithm("q23\r");
        assertEquals("input should be replaced with #", "###\r", result);
    }

    @Test
    public void TestRedactConfigWithNonAsciiChar() throws Exception {
        AbstractInfoType infoType = new PhoneNumber(new RedactConfig("#"));
        String result = infoType.algorithm("भारत");
        System.out.println(result);
        assertEquals("Input should be replaced with #","####",result);

        String result2 = infoType.algorithm("网络");
        System.out.println(result2);
        assertEquals("Input should be replaced with #","##",result2);
    }

    @Test
    public void TestRedactConfigNoReplaceChar() throws Exception {

        AbstractInfoType infoType = new PhoneNumber(new RedactConfig(null));
        String result = infoType.algorithm("hello world");
        LOG.info(result);
        assertEquals("input should be replaced with #", "***********", result);

        String result2 = infoType.algorithm(null);
        LOG.info(result2);
        assertEquals("result should be null", null, result2);
    }

    @Test
    public void TestRedactConfigEmptyReplaceChar() throws Exception {

        AbstractInfoType infoType = new PhoneNumber(new RedactConfig(""));
        String result = infoType.algorithm("hello world");
        LOG.info(result);
        assertEquals("input should be replaced with #", "***********", result);
    }

    @Test
    public void TestRedactConfigMoreThanOneChar() throws Exception {

        AbstractInfoType infoType = new PhoneNumber(new RedactConfig("#*"));
        String result = infoType.algorithm("hello world");
        LOG.info(result);
        assertEquals("input should be replaced with #", "###########", result);

    }

    @Test
    public void TestRedactConfigLongString() throws Exception {

        AbstractInfoType infoType = new PhoneNumber(new RedactConfig("#"));

        String result = infoType.algorithm("This is a paragraph to test hashing for a very long string. The resultant string should not exceed length of 64 byte no matter how big is input string.");
        LOG.info(result);

        String filled = String.format("%0" + 151 + "d", 0).replace('0', '#');
        assertEquals("Hash string length should match", filled, result);

    }

}
