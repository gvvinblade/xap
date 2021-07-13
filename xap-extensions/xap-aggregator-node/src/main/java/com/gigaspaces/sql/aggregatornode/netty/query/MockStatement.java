package com.gigaspaces.sql.aggregatornode.netty.query;

import com.gigaspaces.exception.lrmi.ProtocolException;

import java.util.Iterator;

public class MockStatement<T> extends StatementImpl implements ThrowingSupplier<Iterator<T>, ProtocolException> {
    private final ThrowingSupplier<Iterator<T>, ProtocolException> op;

    public MockStatement(QueryProviderImpl queryProvider, String name, StatementDescription description, ThrowingSupplier<Iterator<T>, ProtocolException> op) {
        super(queryProvider, name, null, null, description);
        this.op = op;
    }

    @Override
    public Iterator<T> apply() throws ProtocolException {
        return op.apply();
    }
}
