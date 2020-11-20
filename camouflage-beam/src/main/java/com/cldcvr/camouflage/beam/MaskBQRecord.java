package com.cldcvr.camouflage.beam;

import com.cldcvr.camouflage.core.exception.CamouflageApiException;
import com.cldcvr.camouflage.core.info.types.AbstractInfoType;
import com.cldcvr.camouflage.core.json.serde.CamouflageSerDe;
import com.cldcvr.camouflage.core.util.MapToInfoType;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;



public class MaskBQRecord extends DoFn<TableRow, TableRow> {

    private final Logger LOG = LoggerFactory.getLogger(MaskBQRecord.class);

    //private final CamouflageSerDe camouflageSerDe;
    private final Map<String, Map<String, Set<AbstractInfoType>>> topicAndColumnInfoTypes;
    private final transient com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();

    public MaskBQRecord(String colMaskInfo) throws IOException {
        BeamCamouflageSerDe beamCamouflageSerDe = mapper.readValue(colMaskInfo, BeamCamouflageSerDe.class);
         topicAndColumnInfoTypes = beamCamouflageSerDe.getList().stream()
                .map(topicAndColumns -> {
                    Map<String, Set<AbstractInfoType>> columnAndAbstractType = new HashMap<>();
                    topicAndColumns.getColumnMetadataList().stream().forEach(r -> {
                        try {
                            columnAndAbstractType.put(r.getColumn(), MapToInfoType.toInfoTypeMapping(r.getDlpTypes()));
                        } catch (CamouflageApiException e) {
                            throw new RuntimeException(e);
                        }
                    });
                    return new TopicAndInfoType(topicAndColumns.getTopic(), columnAndAbstractType);
                }).collect(Collectors.toMap(TopicAndInfoType::getTopic, TopicAndInfoType::getColumnTypes));
    }


    @DoFn.ProcessElement
    public void processElement(ProcessContext c) {
        TableRow row = c.element();
        try {
            String topic = row.get("_connector_name")+"."+row.get("_database_table");
            row.keySet().stream().forEach(x->{
                Set<AbstractInfoType> infotypes = this.topicAndColumnInfoTypes.get(topic).get(x);
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
            LOG.error("TableRow {} failed when applying mask transformation",row,e);
            c.output(row);
        }
    }

    /**
     * {
     * 	"DLPMetadata":[
     * 	{
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
    final class TopicAndInfoType
    {
        private final String topic;
        private final Map<String, Set<AbstractInfoType>> columnTypes;
        public TopicAndInfoType(String topic ,Map<String, Set<AbstractInfoType>> columnTypes)
        {
            this.topic=topic;
            this.columnTypes=columnTypes;
        }

        public String getTopic() {
            return topic;
        }

        public Map<String, Set<AbstractInfoType>> getColumnTypes() {
            return columnTypes;
        }
    }
}
