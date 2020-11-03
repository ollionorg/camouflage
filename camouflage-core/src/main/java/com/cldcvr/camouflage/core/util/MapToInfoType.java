package com.cldcvr.camouflage.core.util;

import com.cldcvr.camouflage.core.exception.CamouflageApiException;
import com.cldcvr.camouflage.core.info.types.AbstractInfoType;
import com.cldcvr.camouflage.core.json.serde.TypeMetadata;

import java.util.*;

public class MapToInfoType {

    public static CamouflageTraits toInfoTypeMapping(String column,List<TypeMetadata> metadata) throws CamouflageApiException {
        Map<String,Set<AbstractInfoType>> returnMap = new HashMap<>();
        Set<AbstractInfoType> types = new LinkedHashSet<AbstractInfoType>();
        try {
            for (int i=0;i<metadata.size();i++)
            {
                types.add(FetchInfoAndMaskTypes.getInfoType(metadata.get(i)));
                returnMap.put(column,types);
            }
        }
        catch (Exception e )
        {
            throw new CamouflageApiException(String.format("Error: [%s] \n Cause: [%s]",
                    String.format("Failed to parse TypeMetadata of `[%s]` to info types", metadata),
                    e.getCause()));
        }
        return new CamouflageTraits(column,types);
    }
}
