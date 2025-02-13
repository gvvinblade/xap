package com.gigaspaces.internal.cluster.node.impl.replica;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

/**
 * @author yaeln nahon
 *
 * Represent a batch to be send to backup during recovery
 *
 * If fifoId != 0 the batch contains entries of fifo / fifo-grouping type
 *
 */
public class SpaceReplicaBatch implements Collection<ISpaceReplicaData>, SmartExternalizable {
    private Collection<ISpaceReplicaData> batch;

    private int fifoId = 0 ;

    public SpaceReplicaBatch() {
    }

    public SpaceReplicaBatch(int batchSize) {
        this.batch = new ArrayList<ISpaceReplicaData>(batchSize);
    }

    public Collection<ISpaceReplicaData> getBatch() {
        return batch;
    }

    public void setBatch(Collection<ISpaceReplicaData> batch) {
        this.batch = batch;
    }

    int getFifoId() {
        return fifoId;
    }

    void setFifoId(int fifoId) {
        this.fifoId = fifoId;
    }

    boolean isFifoBatch(){
        return fifoId != 0;
    }

    @Override
    public int size() {
        return batch.size();
    }

    @Override
    public boolean isEmpty() {
        return batch.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return batch.contains(o);
    }

    @Override
    public Iterator<ISpaceReplicaData> iterator() {
        return batch.iterator();
    }

    @Override
    public Object[] toArray() {
        return batch.toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return batch.toArray(a);
    }

    @Override
    public boolean add(ISpaceReplicaData iSpaceReplicaData) {
        return batch.add(iSpaceReplicaData);
    }

    @Override
    public boolean remove(Object o) {
        return batch.remove(o);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return batch.containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends ISpaceReplicaData> c) {
        return batch.addAll(c);
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return batch.removeAll(c);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return batch.retainAll(c);
    }

    @Override
    public void clear() {
        batch.clear();
    }

    @Override
    public String toString() {
        return "SpaceReplicaBatch{" +
                "batch=" + batch +
                ", fifoId=" + fifoId +
                '}';
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeObject(out, batch);
        IOUtils.writeInt(out, fifoId);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        batch = IOUtils.readObject(in);
        fifoId = IOUtils.readInt(in);
    }
}
