package com.gigaspaces.jdbc.calcite;

import com.gigaspaces.jdbc.calcite.pg.PgTypeDescriptor;
import com.gigaspaces.jdbc.calcite.pg.PgTypeUtils;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.SchemaVersion;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

public abstract class GSAbstractSchema implements Schema {
    @Override
    public RelProtoDataType getType(String name) {
        PgTypeDescriptor type = PgTypeUtils.getTypeByName(name);
        if (type == PgTypeDescriptor.UNKNOWN)
            return null;
        return PgTypeUtils.toRelProtoDataType(type);
    }

    @Override
    public Set<String> getTypeNames() {
        return PgTypeUtils.getTypeNames();
    }

    @Override
    public Collection<Function> getFunctions(String name) {
        return Collections.emptySet();
    }

    @Override
    public Set<String> getFunctionNames() {
        return Collections.emptySet();
    }

    @Override
    public Schema getSubSchema(String name) {
        return null;
    }

    @Override
    public Set<String> getSubSchemaNames() {
        return Collections.emptySet();
    }

    @Override
    public Expression getExpression(SchemaPlus parentSchema, String name) {
        return null;
    }

    @Override
    public boolean isMutable() {
        return false;
    }

    @Override
    public Schema snapshot(SchemaVersion version) {
        return this;
    }
}
