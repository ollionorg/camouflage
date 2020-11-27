package com.cldcvr.camouflage.core.mask.types.impl;

import com.cldcvr.camouflage.core.info.types.AbstractInfoType;
import com.cldcvr.camouflage.core.info.types.impl.PhoneNumber;
import com.cldcvr.camouflage.core.mask.types.impl.HashConfig;
import com.cldcvr.camouflage.core.mask.types.impl.RedactConfig;
import com.cldcvr.camouflage.core.mask.types.impl.ReplaceConfig;
import com.oracle.tools.packager.Log;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;

public class HashConfigTest {

    private final Logger LOG = LoggerFactory.getLogger(HashConfigTest.class);

    @Test
    public void TestHashConfigNullSalt() throws Exception {

        AbstractInfoType infoType = new PhoneNumber(new HashConfig(null));
        String result = infoType.algorithm("hello world");
        LOG.info(result);
        assertEquals("Hash string length should be 64", 64, result.length());
    }

    @Test
    public void TestHashConfigEmptySalt() throws Exception {

        AbstractInfoType infoType = new PhoneNumber(new HashConfig(""));
        String result = infoType.algorithm("hello world");
        LOG.info(result);
        assertEquals("Hash string length should be 64", 64, result.length());
    }

    @Test
    public void TestHashConfigLongSalt() throws Exception {

        AbstractInfoType infoType = new PhoneNumber(new HashConfig("12345678901234567890"));
        String result = infoType.algorithm("hello world");
        LOG.info(result);
        assertEquals("Hash string length should be 64", 64, result.length());
    }
}
