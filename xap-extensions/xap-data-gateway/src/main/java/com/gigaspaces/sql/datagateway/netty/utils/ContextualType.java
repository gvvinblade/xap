package com.gigaspaces.sql.datagateway.netty.utils;

import com.gigaspaces.jdbc.calcite.pg.PgTypeDescriptor;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.EnumMap;

public class ContextualType extends PgType {
    private final EnumMap<SqlTypeName, PgType> typeMap;

    private ContextualType(PgTypeDescriptor desc, EnumMap<SqlTypeName, PgType> typeMap) {
        super(desc);
        this.typeMap = typeMap;
    }

    public PgType getMappedType(RelDataType internal) {
        return typeMap.getOrDefault(internal.getSqlTypeName(), this);
    }

    public static Builder withDescriptor(PgTypeDescriptor desc) {
        return new Builder(desc);
    }

    public static class Builder {
        private final EnumMap<SqlTypeName, PgType> typeMap = new EnumMap<>(SqlTypeName.class);
        private final PgTypeDescriptor desc;

        public Builder(PgTypeDescriptor desc) {
            this.desc = desc;
        }

        public Builder map(SqlTypeName typeName, PgType to) {
            assert desc == to.getDescriptor();
            typeMap.put(typeName, to);
            return this;
        }

        public ContextualType build() {
            return new ContextualType(desc, typeMap);
        }
    }
}
