package com.gigaspaces.sql.datagateway.netty.utils;

import com.gigaspaces.jdbc.calcite.pg.PgTypeDescriptor;
import com.gigaspaces.sql.datagateway.netty.exception.ProtocolException;
import com.gigaspaces.sql.datagateway.netty.query.Session;
import io.netty.buffer.ByteBuf;

public class TypeRecord extends PgType {
    public static final PgType INSTANCE = new TypeRecord();

    public TypeRecord() {
        super(PgTypeDescriptor.RECORD);
    }

    @Override
    protected void asTextInternal(Session session, ByteBuf dst, Object value) throws ProtocolException {
        TypeUtils.writeText(session, dst, session.getRecordSerializer().asString(value));
    }

    @Override
    protected <T> T fromTextInternal(Session session, ByteBuf src) throws ProtocolException {
        return (T) TypeUtils.readText(session, src);
    }
}
