package com.cldcvr.camouflage.beam;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Test {

    public static void main(String[] args) throws JsonProcessingException {
        String json = "{\n" +
                "   \"DLPMetaData\":[\n" +
                "      {\n" +
                "         \"topic\":\"t1\",\n" +
                "         \"columns\":[\n" +
                "            {\n" +
                "               \"column\":\"card_number\",\n" +
                "               \"dlpTypes\":[\n" +
                "                  {\n" +
                "                     \"info_type\":\"PHONE_NUMBER\",\n" +
                "                     \"mask_type\":\"REDACT_CONFIG\",\n" +
                "                     \"replace\":\"*\"\n" +
                "                  }\n" +
                "               ]\n" +
                "            },\n" +
                "            {\n" +
                "               \"column\":\"card_pin\",\n" +
                "               \"dlpTypes\":[\n" +
                "                  {\n" +
                "                     \"info_type\":\"PHONE_NUMBER\",\n" +
                "                     \"mask_type\":\"HASH_CONFIG\",\n" +
                "                     \"salt\":\"somesaltgoeshere\"\n" +
                "                  }\n" +
                "               ]\n" +
                "            }\n" +
                "         ]\n" +
                "      },{\n" +
                "\"topic\":\"t1\",\n" +
                "         \"columns\":[\n" +
                "            {\n" +
                "               \"column\":\"card_number\",\n" +
                "               \"dlpTypes\":[\n" +
                "                  {\n" +
                "                     \"info_type\":\"PHONE_NUMBER\",\n" +
                "                     \"mask_type\":\"REDACT_CONFIG\",\n" +
                "                     \"replace\":\"*\"\n" +
                "                  }\n" +
                "               ]\n" +
                "            },\n" +
                "            {\n" +
                "               \"column\":\"card_pin\",\n" +
                "               \"dlpTypes\":[\n" +
                "                  {\n" +
                "                     \"info_type\":\"PHONE_NUMBER\",\n" +
                "                     \"mask_type\":\"HASH_CONFIG\",\n" +
                "                     \"salt\":\"somesaltgoeshere\"\n" +
                "                  }\n" +
                "               ]\n" +
                "            }\n" +
                "         ]\n" +
                "      }\n" +
                "   ]\n" +
                "}";
        ObjectMapper mapper = new ObjectMapper();
//        Map map = mapper.readValue(json, Map.class);
//        System.out.println(map);
//        System.out.println(map.keySet());
//        System.out.println(map.entrySet());
//        System.out.println(map.get("DLPMetaData"));
        BeamCamouflageSerDe beamCamouflageSerDe = mapper.readValue(json, BeamCamouflageSerDe.class);
        System.out.println(beamCamouflageSerDe);

    }
}
