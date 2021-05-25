package com.gigaspaces.internal.server.space.tiered_storage;

import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.server.space.metadata.SpaceTypeManager;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.gigaspaces.metrics.LongCounter;
import com.gigaspaces.metrics.MetricRegistrator;
import com.j_spaces.core.cache.context.Context;
import com.j_spaces.core.sadapter.ISAdapterIterator;
import com.j_spaces.core.sadapter.SAException;

import java.io.IOException;

public class InternalRDBMSManager {
    private final LongCounter readDisk = new LongCounter();

    public LongCounter getReadDisk() {
        return readDisk;
    }

    public LongCounter getWriteDisk() {
        return writeDisk;
    }

    private final LongCounter writeDisk = new LongCounter();

    public InternalRDBMSManager(InternalRDBMS internalRDBMS) {
        this.internalRDBMS = internalRDBMS;
    }

    InternalRDBMS internalRDBMS;


    public void initialize(String spaceName, String fullMemberName, SpaceTypeManager typeManager, MetricRegistrator registrator) throws SAException{
        internalRDBMS.initialize(spaceName, fullMemberName, typeManager);
    }

    public long getDiskSize() throws SAException, IOException{
        return internalRDBMS.getDiskSize();
    }

    public void createTable(ITypeDesc typeDesc) throws SAException{
        internalRDBMS.createTable(typeDesc);
    }

    /**
     * Inserts a new entry to the internalDiskStorage
     *
     * @param entryHolder entry to insert
     */
    public void insertEntry(Context context,  IEntryHolder entryHolder) throws SAException{
        writeDisk.inc();
        internalRDBMS.insertEntry(context, entryHolder);
    }

    /**
     * updates an entry.
     *
     * @param updatedEntry new content, same UID and class
     */
    public void updateEntry(Context context, IEntryHolder updatedEntry) throws SAException{
        internalRDBMS.updateEntry(context, updatedEntry);
    }

    /**
     * Removes an entry from the  internalDiskStorage
     *
     * @param entryHolder entry to remove
     */
    public boolean removeEntry(Context context, IEntryHolder entryHolder) throws SAException{
        return internalRDBMS.removeEntry(context, entryHolder);
    }

    public IEntryHolder getEntryById(Context context, String typeName, Object id) throws SAException{
        return internalRDBMS.getEntryById(context, typeName, id);
    }

    public IEntryHolder getEntryByUID(Context context, String typeName, String uid) throws SAException{
        return internalRDBMS.getEntryByUID(context, typeName, uid);
    }

    public ISAdapterIterator<IEntryHolder> makeEntriesIter(Context context, String typeName, ITemplateHolder templateHolder) throws SAException{
        readDisk.inc();
        return internalRDBMS.makeEntriesIter(context, typeName, templateHolder);
    }

    public boolean isKnownType(String name){
        return internalRDBMS.isKnownType(name);
    }

    public void shutDown(){
        internalRDBMS.shutDown();
    }
}

