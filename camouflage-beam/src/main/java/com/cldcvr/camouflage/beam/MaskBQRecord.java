package com.cldcvr.camouflage.beam;
import com.cldcvr.camouflage.core.exception.CamouflageApiException;
import com.cldcvr.camouflage.core.info.types.AbstractInfoType;
import com.cldcvr.camouflage.core.json.serde.CamouflageSerDe;
import com.cldcvr.camouflage.core.util.CamouflageTraits;
import com.cldcvr.camouflage.core.util.MapToInfoType;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.transforms.DoFn;
//import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;


public class MaskBQRecord extends DoFn<TableRow, TableRow> {

    private final CamouflageSerDe camouflageSerDe;
    private final Map<String, Set<AbstractInfoType>> traits = new HashMap<>();
    final transient com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();

    public MaskBQRecord(String colMaskInfo) throws IOException {

        camouflageSerDe = mapper.readValue(colMaskInfo, CamouflageSerDe.class);

        camouflageSerDe.getDlpMetadata().stream().forEach(r -> {
            try {
                traits.put(r.getColumn(), MapToInfoType.toInfoTypeMapping(r.getColumn(), r.getDlpTypes()));
            } catch (CamouflageApiException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @DoFn.ProcessElement
    public void processElement(ProcessContext c) {
        try {
            TableRow row = c.element();

            row.keySet().stream().forEach(x->{
                Set<AbstractInfoType> infotypes = this.traits.get(x);
                if (infotypes != null && infotypes.size() > 0 ) {
                    Iterator<AbstractInfoType> it = infotypes.iterator();
                    String value = (String) row.get(x);

                    while (it.hasNext()) {
                        AbstractInfoType infoType = it.next();
                        value = infoType.getMaskStrategy().applyMaskStrategy(value, infoType.regex());
                    }
                    row.set(x, value);
                }
            });
            c.output(row);
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
