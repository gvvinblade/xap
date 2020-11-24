/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.gigaspaces.internal.server.storage;

import com.gigaspaces.document.DocumentProperties;
import com.gigaspaces.internal.metadata.EntryTypeDesc;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.metadata.PropertyInfo;
import com.gigaspaces.utils.Pair;
import com.j_spaces.core.server.transaction.EntryXtnInfo;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Map;

/***
 * @author Yael Nahon
 * @since 15.8
 */
@com.gigaspaces.api.InternalApi
public class HybridBinaryEntryData implements IBinaryEntryData {
    protected final EntryTypeDesc _entryTypeDesc;
    protected final int _versionID;
    protected final long _expirationTime;
    protected final EntryXtnInfo _entryTxnInfo;
    protected Map<String, Object> _dynamicProperties;
    private byte[] serializedFields;
    private Object[] nonSerializedFields;


    public HybridBinaryEntryData(Object[] nonSerializedFields, Map<String, Object> dynamicProperties, EntryTypeDesc entryTypeDesc, int version, long expirationTime, EntryXtnInfo entryXtnInfo) {
        this(extractFields(nonSerializedFields, entryTypeDesc.getTypeDesc()),dynamicProperties, entryTypeDesc, version, expirationTime, entryXtnInfo);
    }

    public HybridBinaryEntryData(Pair<Object[],byte[]> fields, Map<String, Object> dynamicProperties, EntryTypeDesc entryTypeDesc, int version,
                                 long expirationTime, EntryXtnInfo entryXtnInfo) {
        this._entryTypeDesc = entryTypeDesc;
        this._versionID = version;
        this._expirationTime = expirationTime;
        this._entryTxnInfo = entryXtnInfo;
        this._dynamicProperties = dynamicProperties;
        this.nonSerializedFields = fields.getFirst();
        this.serializedFields = fields.getSecond();
    }

    private static Pair<Object[],byte[]> extractFields(Object[] fields, ITypeDesc typeDesc) {
        Object[] nonSerializedFields = new Object[typeDesc.getNonSerializedProperties().length];
        Object[] serializedFields = new Object[typeDesc.getSerializedProperties().length];
        int nonSerializedFieldsIndex = 0;
        int serializedFieldsIndex = 0;
        for (PropertyInfo property : typeDesc.getProperties()) {
            if(property.getStorageType() != null && property.isBinarySpaceProperty()){
                serializedFields[serializedFieldsIndex] = fields[typeDesc.getFixedPropertyPosition(property.getName())];
                serializedFieldsIndex++;
            } else {
                nonSerializedFields[nonSerializedFieldsIndex] = fields[typeDesc.getFixedPropertyPosition(property.getName())];
                nonSerializedFieldsIndex++;
            }
        }
        try {
            return new Pair<>(nonSerializedFields, typeDesc.getClassBinaryStorageAdapter().toBinary(typeDesc, serializedFields));
        } catch (IOException e) {
            throw new UncheckedIOException("com.gigaspaces.internal.server.storage.BinaryEntryData.serializeFields failed", e);
        }
    }

    protected static byte[] serializeFields(Object[] fieldsValues, ITypeDesc typeDesc) {
        try {
            return typeDesc.getClassBinaryStorageAdapter().toBinary(typeDesc, fieldsValues);
        } catch (IOException e) {
            throw new UncheckedIOException("com.gigaspaces.internal.server.storage.BinaryEntryData.serializeFields failed", e);
        }
    }


    @Override
    public ITransactionalEntryData createCopy(int newVersion, long newExpiration, EntryXtnInfo newEntryXtnInfo, boolean shallowCloneData) {
        byte[] serializedData = shallowCloneData ? Arrays.copyOf(this.serializedFields, this.serializedFields.length) : serializedFields;
        Object[] nonSerializedata = shallowCloneData ? Arrays.copyOf(this.nonSerializedFields, this.nonSerializedFields.length) : nonSerializedFields;
        return new HybridBinaryEntryData(new Pair<>(nonSerializedFields, serializedFields),_dynamicProperties, this._entryTypeDesc, newVersion, newExpiration, newEntryXtnInfo);
    }

    @Override
    public ITransactionalEntryData createCopy(IEntryData newEntryData, long newExpirationTime) {
        if(newEntryData instanceof HybridBinaryEntryData){
            return new HybridBinaryEntryData(new Pair<>(((HybridBinaryEntryData) newEntryData).getNonSerializedFields(), ((HybridBinaryEntryData) newEntryData).getSerializedFields()),
                    newEntryData.getDynamicProperties(), newEntryData.getEntryTypeDesc(), newEntryData.getVersion(), newExpirationTime,
                    copyTxnInfo(false, false));
        } else {
            return new HybridBinaryEntryData(newEntryData.getFixedPropertiesValues(),newEntryData.getDynamicProperties(), newEntryData.getEntryTypeDesc(), newEntryData.getVersion(), newExpirationTime,
                    copyTxnInfo(false, false));
        }
    }

    public Object[] getNonSerializedFields() {
        return this.nonSerializedFields;
    }


    @Override
    public Object getFixedPropertyValue(int index) {
        if(_entryTypeDesc.getTypeDesc().
                isBinaryProperty(index)){
            try {
                return _entryTypeDesc.getTypeDesc().getClassBinaryStorageAdapter()
                        .getFieldAtIndex(_entryTypeDesc.getTypeDesc(), serializedFields, _entryTypeDesc.getTypeDesc().findHybridIndex(index));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            } catch (ClassNotFoundException e) {
                throw new IllegalStateException(e);
            }
        }else {
            return nonSerializedFields[_entryTypeDesc.getTypeDesc().findHybridIndex(index)];
        }
    }

    @Override
    public void setFixedPropertyValue(int index, Object value) {
        if(_entryTypeDesc.getTypeDesc().isBinaryProperty(index)){
            try {
                _entryTypeDesc.getTypeDesc().getClassBinaryStorageAdapter()
                        .modifyField(_entryTypeDesc.getTypeDesc(), serializedFields, _entryTypeDesc.getTypeDesc().findHybridIndex(index), value);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            } catch (ClassNotFoundException e) {
                throw new IllegalStateException(e);
            }
        }else {
            nonSerializedFields[_entryTypeDesc.getTypeDesc().findHybridIndex(index)] = value;
        }
    }

    @Override
    public Object[] getFixedPropertiesValues() {
        Object[] fields = new Object[_entryTypeDesc.getTypeDesc().getProperties().length];
        try {
            Object[] deserializedFields = _entryTypeDesc.getTypeDesc().getClassBinaryStorageAdapter().fromBinary(_entryTypeDesc.getTypeDesc(), serializedFields);
            int i = 0;
            for (PropertyInfo property : _entryTypeDesc.getTypeDesc().getProperties()) {
                if(property.isBinarySpaceProperty()){
                    fields[i] = deserializedFields[_entryTypeDesc.getTypeDesc().findHybridIndex(i)];
                } else {
                    fields[i] = nonSerializedFields[_entryTypeDesc.getTypeDesc().findHybridIndex(i)];
                }
                i++;
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(e);
        }
        return fields;
    }

    @Override
    public void setFixedPropertyValues(Object[] values) {
        if (values.length != getNumOfFixedProperties()) {
            throw new IllegalArgumentException("Cannot substitute fixed property values with array of different size!");
        }
        Pair<Object[], byte[]> fields = extractFields(values, this._entryTypeDesc.getTypeDesc());
        nonSerializedFields = fields.getFirst();
        serializedFields = fields.getSecond();
    }


    public void setFixedPropertyValues(Object[] nonSerializedFields, byte[] serializedFields) {
        this.nonSerializedFields = nonSerializedFields;
        this.serializedFields = serializedFields;
    }

    @Override
    public byte[] getSerializedFields() {
        return serializedFields;
    }

    @Override
    public boolean isEqualProperties(IBinaryEntryData old){
        return serializedFields == old.getSerializedFields() &&
                nonSerializedFields == ((HybridBinaryEntryData) old).nonSerializedFields;
    }

    @Override
    public EntryTypeDesc getEntryTypeDesc() {
        return _entryTypeDesc;
    }

    @Override
    public int getVersion() {
        return _versionID;
    }

    @Override
    public long getExpirationTime() {
        return _expirationTime;
    }

    @Override
    public EntryXtnInfo getEntryXtnInfo() {
        return _entryTxnInfo;
    }

    @Override
    public EntryDataType getEntryDataType() {
        return EntryDataType.FLAT;
    }

    @Override
    public Map<String, Object> getDynamicProperties() {
        return _dynamicProperties;
    }

    @Override
    public void setDynamicProperties(Map<String, Object> dynamicProperties) {
        _dynamicProperties = dynamicProperties;
    }

    @Override
    public void setDynamicPropertyValue(String propertyName, Object value) {
        if (!_entryTypeDesc.getTypeDesc().supportsDynamicProperties())
            throw new UnsupportedOperationException(_entryTypeDesc.getTypeDesc().getTypeName() + " does not support dynamic properties");

        if (_dynamicProperties == null)
            _dynamicProperties = new DocumentProperties();

        _dynamicProperties.put(propertyName, value);
    }
}