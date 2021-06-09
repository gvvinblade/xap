package com.gigaspaces.jdbc.model.result;

import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.jdbc.model.table.OrderColumn;
import com.gigaspaces.jdbc.model.table.QueryColumn;

import java.util.List;

public class TableRow implements Comparable<TableRow>{
    private final QueryColumn[] columns;
    private final Object[] values;
    private final OrderColumn[] orderColumns;
    private final Object[] orderValues;
    private final QueryColumn[] groupByColumns;

    private final Object[] groupByValues;

    public TableRow(QueryColumn[] columns, Object[] values) {
        this.columns = columns;
        this.values = values;
        this.orderColumns = new OrderColumn[0];
        this.orderValues = new Object[0];
        this.groupByColumns = new QueryColumn[0];
        this.groupByValues = new Object[0];
    }

    public TableRow(IEntryPacket x, List<QueryColumn> queryColumns, List<OrderColumn> orderColumns, List<QueryColumn> groupByColumns) {
        this.columns = queryColumns.toArray(new QueryColumn[0]);
        values = new Object[columns.length];
        for (int i = 0; i < queryColumns.size(); i++) {
            values[i] = getEntryPacketValue( x, queryColumns.get(i) );
        }

        this.orderColumns = orderColumns.toArray(new OrderColumn[0]);
        orderValues = new Object[this.orderColumns.length];
        for (int i = 0; i < orderColumns.size(); i++) {
            orderValues[i] = getEntryPacketValue( x, orderColumns.get(i) );
        }

        this.groupByColumns = groupByColumns.toArray(new QueryColumn[0]);
        groupByValues = new Object[this.groupByColumns.length];
        for (int i = 0; i < groupByColumns.size(); i++) {
            groupByValues[i] = getEntryPacketValue( x, groupByColumns.get(i) );
        }
    }

    public TableRow(List<QueryColumn> queryColumns, List<OrderColumn> orderColumns, List<QueryColumn> groupByColumns) {
        this.columns = queryColumns.toArray(new QueryColumn[0]);
        values = new Object[columns.length];
        for (int i = 0; i < queryColumns.size(); i++) {
            values[i] = queryColumns.get(i).getCurrentValue();
        }

        this.orderColumns = orderColumns.toArray(new OrderColumn[0]);
        orderValues = new Object[this.orderColumns.length];
        for (int i = 0; i < orderColumns.size(); i++) {
            orderValues[i] = orderColumns.get(i).getCurrentValue();
        }

        this.groupByColumns = groupByColumns.toArray(new QueryColumn[0]);
        groupByValues = new Object[this.groupByColumns.length];
        for (int i = 0; i < groupByColumns.size(); i++) {
            groupByValues[i] = groupByColumns.get(i).getCurrentValue();
        }
    }

    public TableRow(TableRow row, List<QueryColumn> queryColumns, List<OrderColumn> orderColumns, List<QueryColumn> groupByColumns) {
        this.columns = queryColumns.toArray(new QueryColumn[0]);
        values = new Object[columns.length];
        for (int i = 0; i < queryColumns.size(); i++) {
            values[i] = row.getPropertyValue(queryColumns.get(i));
        }

        this.orderColumns = orderColumns.toArray(new OrderColumn[0]);
        orderValues = new Object[this.orderColumns.length];
        for (int i = 0; i < orderColumns.size(); i++) {
            orderValues[i] = row.getPropertyValue(orderColumns.get(i).getName());
        }

        this.groupByColumns = groupByColumns.toArray(new QueryColumn[0]);
        groupByValues = new Object[this.groupByColumns.length];
        for (int i = 0; i < groupByColumns.size(); i++) {
            groupByValues[i] = row.getPropertyValue(groupByColumns.get(i).getName());
        }
    }

    public Object getPropertyValue(int index) {
        return values[index];
    }

    public Object getPropertyValue(QueryColumn column) {
        for (int i = 0; i < columns.length; i++) {
            if (columns[i].equals(column)) {
                return values[i];
            }
        }
        return null;
    }

    public Object getPropertyValue(OrderColumn column) {
        for (int i = 0; i < orderColumns.length; i++) {
            if (orderColumns[i].equals(column)) {
                return orderValues[i];
            }
        }
        return null;
    }

    public Object getPropertyValue(String name) {
        for (int i = 0; i < columns.length; i++) {
            if (columns[i].getName().equals(name)) {
                return values[i];
            }
        }
        return null;
    }

    @Override
    public int compareTo(TableRow other) {
        int results = 0;
        for (OrderColumn orderCol : this.orderColumns) {
            Comparable first = (Comparable) this.getPropertyValue(orderCol);
            Comparable second = (Comparable) other.getPropertyValue(orderCol);

            if (first == second) {
                continue;
            }
            if (first == null) {
                return orderCol.isNullsLast() ? 1 : -1;
            }
            if (second == null) {
                return orderCol.isNullsLast() ? -1 : 1;
            }
            results = first.compareTo(second);
            if (results != 0) {
                return orderCol.isAsc() ? results : -results;
            }
        }
        return results;
    }

    public Object[] getGroupByValues() {
        return groupByValues;
    }

    private Object getEntryPacketValue( IEntryPacket entryPacket, QueryColumn queryColumn ){

        Object value;
        if (queryColumn.isUUID()) {
            value = entryPacket.getUID();
        } else if (entryPacket.getTypeDescriptor().getIdPropertyName().equalsIgnoreCase(queryColumn.getName())) {
            value = entryPacket.getID();
        } else {
            value = entryPacket.getPropertyValue(queryColumn.getName());
        }

        return value;
    }
}