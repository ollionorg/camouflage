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
    public void TestRedactConfigNoReplaceChar() throws Exception {

        AbstractInfoType infoType = new PhoneNumber(new RedactConfig(null));
        String result = infoType.algorithm("hello world");
        LOG.info(result);
        assertEquals("input should be replaced with #", "***********", result);
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

}
