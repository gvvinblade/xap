package com.gigaspaces.sql.datagateway.netty.utils;

import com.gigaspaces.jdbc.calcite.pg.PgTypeDescriptor;
import com.gigaspaces.sql.datagateway.netty.exception.ProtocolException;
import com.gigaspaces.sql.datagateway.netty.query.Session;
import io.netty.buffer.ByteBuf;

import java.math.BigDecimal;

import static com.gigaspaces.sql.datagateway.netty.utils.DateTimeUtils.asYearMonths;
import static com.gigaspaces.sql.datagateway.netty.utils.DateTimeUtils.yearMonthsAsText;
import static com.gigaspaces.sql.datagateway.netty.utils.DateTimeUtils.writeYearMonths;
import static com.gigaspaces.sql.datagateway.netty.utils.TypeUtils.checkLen;
import static com.gigaspaces.sql.datagateway.netty.utils.TypeUtils.checkType;
import static com.gigaspaces.sql.datagateway.netty.utils.TypeUtils.readText;
import static com.gigaspaces.sql.datagateway.netty.utils.TypeUtils.writeText;

public class TypeIntervalYearMonth extends PgType {
    public static final PgType INSTANCE = new TypeIntervalYearMonth();

    public TypeIntervalYearMonth() {
        super(PgTypeDescriptor.INTERVAL);
    }

    @Override
    protected void asTextInternal(Session session, ByteBuf dst, Object value) throws ProtocolException {
        checkType(value, BigDecimal.class);
        writeText(session, dst, yearMonthsAsText((BigDecimal) value));
    }

    @Override @SuppressWarnings("unchecked")
    protected <T> T fromTextInternal(Session session, ByteBuf src) throws ProtocolException {
        return (T) asYearMonths(readText(session, src));
    }

    @Override
    protected void asBinaryInternal(Session session, ByteBuf dst, Object value) throws ProtocolException {
        checkType(value, BigDecimal.class);
        dst.writeInt(16); // write length
        writeYearMonths(dst, (BigDecimal) value);
    }

    @Override @SuppressWarnings("unchecked")
    protected <T> T fromBinaryInternal(Session session, ByteBuf src) throws ProtocolException {
        checkLen(src, 16);
        return (T) asYearMonths(src.readLong(), src.readInt(), src.readInt());
    }}
