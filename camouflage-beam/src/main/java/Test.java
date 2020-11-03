import com.cldcvr.camouflage.core.json.serde.CamouflageSerDe;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class Test {

    public static void main(String[] args) throws IOException {
         final ObjectMapper mapper = new ObjectMapper();
         String json ="{\n" +
                 "\t\"DLPMetadata\":[{\n" +
                 "\t\t\"column\":\"card_number\",\n" +
                 "\t\t\"dlpTypes\":[{\n" +
                 "\t\t\t\"info_type\":\"PHONE_NUMBER\",\n" +
                 "          \"mask_type\":\"REDACT_CONFIG\",\n" +
                 "           \"replace\":\"*\"\n" +
                 "\t\t}\n" +
                 "       ]\n" +
                 "\t},\n" +
                 "\t{\n" +
                 "\t\t\"column\":\"card_pin\",\n" +
                 "\t\t\"dlpTypes\":[{\n" +
                 "\t\t\t\"info_type\":\"PHONE_NUMBER\",\n" +
                 "          \"mask_type\":\"HASH\",\n" +
                 "           \"salt\":\"somesaltgoeshere\"\n" +
                 "\t\t}\n" +
                 "       ]\n" +
                 "\t}\n" +
                 "]\n" +
                 "}";
        System.out.println(json);
        CamouflageSerDe camouflageSerDe = mapper.readValue(json, CamouflageSerDe.class);
        System.out.println("JSON "+ camouflageSerDe);
        mapper.readValue("{\"emp_name\":\"Taher\",\"emp_id\":\"1\"}",Emp.class);

    }

    static class Emp
    {
        private final String name;
        private final int id;

        Emp(@JsonProperty("emp_name") String name, @JsonProperty("emp_id") int id) {
            this.name = name;
            this.id = id;
        }

        public String getName() {
            return name;
        }

        @Override
        public String toString() {
            return "Emp{" +
                    "name='" + name + '\'' +
                    ", id=" + id +
                    '}';
        }
    }
}
