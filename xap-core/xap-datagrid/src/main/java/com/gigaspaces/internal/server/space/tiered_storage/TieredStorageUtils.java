package com.gigaspaces.internal.server.space.tiered_storage;

import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.gigaspaces.internal.server.space.SpaceEngine;
import com.gigaspaces.internal.server.storage.EntryTieredMetaData;
import com.gigaspaces.internal.server.storage.IEntryData;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.j_spaces.core.cache.context.Context;
import com.j_spaces.core.cache.context.TemplateMatchTier;
import com.j_spaces.core.cache.context.TieredState;
import com.j_spaces.core.sadapter.SAException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class TieredStorageUtils {
    private static Logger logger = LoggerFactory.getLogger(TieredStorageUtils.class);

    public static Map<Object, EntryTieredMetaData> getEntriesTieredMetaDataByIds(SpaceEngine space, String typeName, Object[] ids) throws Exception {
        Map<Object, EntryTieredMetaData> entryTieredMetaDataMap =  new HashMap<>();
        if (!space.isTieredStorage()) {
            throw new Exception("Tiered storage undefined");
        }
        Context context = null;
        try{
            context = space.getCacheManager().getCacheContext();
            for (Object id : ids) {
                entryTieredMetaDataMap.put(id, getEntryTieredMetaDataById(space, typeName, id, context));
            }
        } finally{
            space.getCacheManager().freeCacheContext(context);
        }
        return entryTieredMetaDataMap;
    }

    private static EntryTieredMetaData getEntryTieredMetaDataById(SpaceEngine space, String typeName, Object id, Context context) {
        EntryTieredMetaData entryTieredMetaData = new EntryTieredMetaData();
        IServerTypeDesc typeDesc = space.getTypeManager().getServerTypeDesc(typeName);
        IEntryHolder hotEntryHolder = space.getCacheManager().getEntryByIdFromPureCache(id, typeDesc);
        IEntryHolder coldEntryHolder = null;

        try {
            coldEntryHolder = space.getTieredStorageManager().getInternalStorage().getEntry(context, typeDesc.getTypeName(), id);
        } catch (SAException e) { //entry doesn't exist in cold tier
        }

        if (hotEntryHolder != null){
            if (coldEntryHolder == null){
                entryTieredMetaData.setTieredState(TieredState.TIERED_HOT);
            } else {
                entryTieredMetaData.setTieredState(TieredState.TIERED_HOT_AND_COLD);
                entryTieredMetaData.setIdenticalToCache(isIdenticalToCache(hotEntryHolder.getEntryData(),(coldEntryHolder.getEntryData())));
            }
        } else {
            if (coldEntryHolder != null) {
                entryTieredMetaData.setTieredState(TieredState.TIERED_COLD);
            } //else- entry doesn't exist
        }
        return entryTieredMetaData;
    }

    private static boolean isIdenticalToCache(IEntryData hotEntry, IEntryData coldEntry){
        if(hotEntry.getNumOfFixedProperties() != coldEntry.getNumOfFixedProperties()){
            return false;
        }
        for(int i = 0; i < hotEntry.getNumOfFixedProperties(); ++i){
            Object hotValue = hotEntry.getFixedPropertiesValues()[i];
            Object coldValue = coldEntry.getFixedPropertiesValues()[i];
            if(hotValue == null || coldValue == null){
                return hotValue == coldValue;
            }
            if(!hotValue.equals(coldValue)){
                logger.warn("Failed to have consistency between hot and cold tier for id: " +
                        hotEntry.getEntryDataType().name() + " Hot: " + hotValue + " Cold: " + coldValue);

                return false;
            }
        }
        return true;
    }

    public static List<String> getTiersAsList(TemplateMatchTier templateTieredState) {
        switch (templateTieredState){
            case MATCH_HOT:
                return Collections.singletonList("HOT");
            case MATCH_COLD:
                return Collections.singletonList("COLD");
            case MATCH_HOT_AND_COLD:
                return Arrays.asList("HOT", "COLD");
        }

        throw new IllegalStateException("Should be unreachable");
    }
}
