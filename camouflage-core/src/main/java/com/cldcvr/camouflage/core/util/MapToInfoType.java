package com.cldcvr.camouflage.core.util;

import com.cldcvr.camouflage.core.exception.CamouflageApiException;
import com.cldcvr.camouflage.core.info.types.AbstractInfoType;
import com.cldcvr.camouflage.core.json.serde.TypeMetadata;
import java.io.Serializable;
import java.util.*;

/**
 * Constructs {@link AbstractInfoType} from {@link TypeMetadata}
 */
public class MapToInfoType implements Serializable {

    /**
     * Accept a List of {@link TypeMetadata} and gives a Set of {@link AbstractInfoType}
     * @param metadata List of {@link TypeMetadata}
     * @return Set of AbstractInfoType
     * @throws CamouflageApiException
     */
    public static Set<AbstractInfoType> toInfoTypeMapping(List<TypeMetadata> metadata) throws CamouflageApiException {
        Set<AbstractInfoType> types = new LinkedHashSet<AbstractInfoType>();
        try {
            for (int i=0;i<metadata.size();i++)
            {
                types.add(FetchInfoAndMaskTypes.getInfoType(metadata.get(i)));
            }
        }
        catch (Exception e )
        {
            throw new CamouflageApiException(String.format("Error: [%s] \n Cause: [%s] \n Message: [%s]",
                    String.format("Failed to parse TypeMetadata of `[%s]` to info types", metadata),
                    e.getCause(),e.getMessage()));
        }
        return types;
    }
}
