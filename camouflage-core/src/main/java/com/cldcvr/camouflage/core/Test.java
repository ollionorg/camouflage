package com.cldcvr.camouflage.core;

import com.cldcvr.camouflage.core.info.types.AbstractInfoType;
import com.cldcvr.camouflage.core.info.types.impl.PhoneNumber;
import com.cldcvr.camouflage.core.mask.types.impl.HashConfig;
import com.cldcvr.camouflage.core.mask.types.impl.RedactConfig;
import com.cldcvr.camouflage.core.mask.types.impl.ReplaceConfig;

public class Test {

    public static void main(String[] args) {

        AbstractInfoType phHc = new PhoneNumber(new HashConfig("123mysalt123"));
        System.out.println(phHc.algorithm("hello world"));

        AbstractInfoType ph = new PhoneNumber(new RedactConfig("#"));
        System.out.println(ph.algorithm("823423423"));

        AbstractInfoType ph2 = new PhoneNumber(null);
        System.out.println(ph.algorithm("823423423"));

    }
}
