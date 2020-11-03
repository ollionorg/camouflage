import com.cldcvr.camouflage.core.exception.CamouflageApiException;
import com.cldcvr.camouflage.core.json.serde.CamouflageSerDe;
import com.cldcvr.camouflage.core.util.CamouflageTraits;
import com.cldcvr.camouflage.core.util.MapToInfoType;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.transforms.DoFn;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;


public class MaskBQRecord extends DoFn<TableRow, TableRow> {

    private final Map<String, Object> maskInfo;
    private final ObjectMapper mapper = new ObjectMapper();
    private final CamouflageSerDe camouflageSerDe;
    private final Set<CamouflageTraits> traits;

    public MaskBQRecord(Map<String, Object> kv) throws IOException {
        maskInfo = kv;
        camouflageSerDe = mapper.readValue(mapper.writeValueAsString(maskInfo), CamouflageSerDe.class);
        traits = camouflageSerDe.getDlpMetadata().stream().map(r -> {
            try {
                return MapToInfoType.toInfoTypeMapping(r.getColumn(), r.getDlpTypes());
            } catch (CamouflageApiException e) {
                throw new RuntimeException(e);
            }
        }).collect(Collectors.toSet());
    }

    @DoFn.ProcessElement
    public void processElement(ProcessContext c) {
        try {
            TableRow row = c.element();
        } catch (Exception e) {

        }
    }
    /**
     * {
     * 	"DLPMetadata":[{
     * 		"column":"card_number",
     * 		"dlpTypes":[{
     * 			"info_type":"PHONE_NUMBER",
     *           "mask_type":"REDACT_CONFIG",
     *            "replace":"*"
     *                }
     *        ]* 	},
     * 	{
     * 		"column":"card_pin",
     * 		"dlpTypes":[{
     * 			"info_type":"PHONE_NUMBER",
     *           "mask_type":"HASH",
     *            "salt":"somesaltgoeshere"
     * 		}
     *        ]
     * 	}
     * ]
     * }
     */
}
