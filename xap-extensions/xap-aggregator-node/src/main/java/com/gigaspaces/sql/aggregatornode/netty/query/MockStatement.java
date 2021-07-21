package com.gigaspaces.sql.aggregatornode.netty.query;

import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.metadata.PropertyInfo;
import com.gigaspaces.jdbc.calcite.GSOptimizer;
import com.gigaspaces.jdbc.calcite.GSTable;
import com.gigaspaces.jdbc.calcite.pg.PgOidGenerator;
import com.gigaspaces.jdbc.calcite.pg.PgTypeDescriptor;
import com.gigaspaces.jdbc.calcite.pg.PgTypeUtils;
import com.gigaspaces.sql.aggregatornode.netty.exception.NonBreakingException;
import com.gigaspaces.sql.aggregatornode.netty.exception.ProtocolException;
import com.gigaspaces.sql.aggregatornode.netty.utils.ErrorCodes;
import com.gigaspaces.sql.aggregatornode.netty.utils.TypeUtils;
import com.google.common.collect.ImmutableList;
import com.j_spaces.core.admin.IRemoteJSpaceAdmin;
import com.j_spaces.core.admin.SpaceRuntimeInfo;
import com.j_spaces.jdbc.SQLUtil;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.jetbrains.annotations.NotNull;

import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MockStatement implements Statement {
    private static final String QUERY_TABLES = "select relname, nspname, relkind from pg_catalog.pg_class c, pg_catalog.pg_namespace n where relkind in ('r', 'v', 'm', 'f', 'p') and nspname not in ('pg_catalog', 'information_schema', 'pg_toast', 'pg_temp_1') and n.oid = relnamespace order by nspname, relname";
    private final static String QUERY_ATTRIBUTES = "\\Qselect n.nspname, c.relname, a.attname, a.atttypid, t.typname, a.attnum, a.attlen, a.atttypmod, a.attnotnull, c.relhasrules, c.relkind, c.oid, pg_get_expr(d.adbin, d.adrelid), case t.typtype when 'd' then t.typbasetype else 0 end, t.typtypmod, c.relhasoids, '', c.relhassubclass from (((pg_catalog.pg_class c inner join pg_catalog.pg_namespace n on n.oid = c.relnamespace and c.relname like E'\\E(.+)\\Q' and n.nspname like E'\\E(.+)\\Q') inner join pg_catalog.pg_attribute a on (not a.attisdropped) and a.attnum > 0 and a.attrelid = c.oid) inner join pg_catalog.pg_type t on t.oid = a.atttypid) left outer join pg_attrdef d on a.atthasdef and d.adrelid = a.attrelid and d.adnum = a.attnum order by n.nspname, c.relname, attnum\\E";
    private final static String QUERY_INDEXED_ATTRIBUTES_1 = "\\Qselect ta.attname, ia.attnum, ic.relname, n.nspname, tc.relname from pg_catalog.pg_attribute ta, pg_catalog.pg_attribute ia, pg_catalog.pg_class tc, pg_catalog.pg_index i, pg_catalog.pg_namespace n, pg_catalog.pg_class ic where tc.relname = E'\\E(.*)\\Q' AND n.nspname = E'\\E(.*)\\Q' AND tc.oid = i.indrelid AND n.oid = tc.relnamespace AND i.indisprimary = 't' AND ia.attrelid = i.indexrelid AND ta.attrelid = i.indrelid AND ta.attnum = i.indkey[ia.attnum-1] AND (NOT ta.attisdropped) AND (NOT ia.attisdropped) AND ic.oid = i.indexrelid order by ia.attnum\\E";
    private final static String QUERY_INDEXED_ATTRIBUTES_2 = "\\Qselect ta.attname, ia.attnum, ic.relname, n.nspname, NULL from pg_catalog.pg_attribute ta, pg_catalog.pg_attribute ia, pg_catalog.pg_class ic, pg_catalog.pg_index i, pg_catalog.pg_namespace n where ic.relname = E'\\E(.*)\\Q' AND n.nspname = E'\\E(.*)\\Q' AND ic.oid = i.indexrelid AND n.oid = ic.relnamespace AND ia.attrelid = i.indexrelid AND ta.attrelid = i.indrelid AND ta.attnum = i.indkey[ia.attnum-1] AND (NOT ta.attisdropped) AND (NOT ia.attisdropped) order by ia.attnum\\E";
    private final static String QUERY_CONSTRAINT_1 = "\\Qselect\t'\\E(.*)\\Q'::name as \"PKTABLE_CAT\",\n" +
            "\tn2.nspname as \"PKTABLE_SCHEM\",\n" +
            "\tc2.relname as \"PKTABLE_NAME\",\n" +
            "\ta2.attname as \"PKCOLUMN_NAME\",\n" +
            "\t'\\E(.*)\\Q'::name as \"FKTABLE_CAT\",\n" +
            "\tn1.nspname as \"FKTABLE_SCHEM\",\n" +
            "\tc1.relname as \"FKTABLE_NAME\",\n" +
            "\ta1.attname as \"FKCOLUMN_NAME\",\n" +
            "\ti::int2 as \"KEY_SEQ\",\n" +
            "\tcase ref.confupdtype\n" +
            "\t\twhen 'c' then 0::int2\n" +
            "\t\twhen 'n' then 2::int2\n" +
            "\t\twhen 'd' then 4::int2\n" +
            "\t\twhen 'r' then 1::int2\n" +
            "\t\telse 3::int2\n" +
            "\tend as \"UPDATE_RULE\",\n" +
            "\tcase ref.confdeltype\n" +
            "\t\twhen 'c' then 0::int2\n" +
            "\t\twhen 'n' then 2::int2\n" +
            "\t\twhen 'd' then 4::int2\n" +
            "\t\twhen 'r' then 1::int2\n" +
            "\t\telse 3::int2\n" +
            "\tend as \"DELETE_RULE\",\n" +
            "\tref.conname as \"FK_NAME\",\n" +
            "\tcn.conname as \"PK_NAME\",\n" +
            "\tcase\n" +
            "\t\twhen ref.condeferrable then\n" +
            "\t\t\tcase\n" +
            "\t\t\twhen ref.condeferred then 5::int2\n" +
            "\t\t\telse 6::int2\n" +
            "\t\t\tend\n" +
            "\t\telse 7::int2\n" +
            "\tend as \"DEFERRABILITY\"\n" +
            " from\n" +
            " ((((((( (select cn.oid, conrelid, conkey, confrelid, confkey,\n" +
            "\t generate_series(array_lower(conkey, 1), array_upper(conkey, 1)) as i,\n" +
            "\t confupdtype, confdeltype, conname,\n" +
            "\t condeferrable, condeferred\n" +
            "  from pg_catalog.pg_constraint cn,\n" +
            "\tpg_catalog.pg_class c,\n" +
            "\tpg_catalog.pg_namespace n\n" +
            "  where contype = 'f' \n" +
            "   and  conrelid = c.oid\n" +
            "   and  relname = E'\\E(.*)\\Q'\n" +
            "   and  n.oid = c.relnamespace\n" +
            "   and  n.nspname = E'\\E(.*)\\Q'\n" +
            " ) ref\n" +
            " inner join pg_catalog.pg_class c1\n" +
            "  on c1.oid = ref.conrelid)\n" +
            " inner join pg_catalog.pg_namespace n1\n" +
            "  on  n1.oid = c1.relnamespace)\n" +
            " inner join pg_catalog.pg_attribute a1\n" +
            "  on  a1.attrelid = c1.oid\n" +
            "  and  a1.attnum = conkey[i])\n" +
            " inner join pg_catalog.pg_class c2\n" +
            "  on  c2.oid = ref.confrelid)\n" +
            " inner join pg_catalog.pg_namespace n2\n" +
            "  on  n2.oid = c2.relnamespace)\n" +
            " inner join pg_catalog.pg_attribute a2\n" +
            "  on  a2.attrelid = c2.oid\n" +
            "  and  a2.attnum = confkey[i])\n" +
            " left outer join pg_catalog.pg_constraint cn\n" +
            "  on cn.conrelid = ref.confrelid\n" +
            "  and cn.contype = 'p')\n" +
            "  order by ref.oid, ref.i\\E";

    public static final String QUERY_CONSTRAINT_2 = "\\Qselect\t'\\E(.*)\\Q'::name as \"PKTABLE_CAT\",\n" +
            "\tn2.nspname as \"PKTABLE_SCHEM\",\n" +
            "\tc2.relname as \"PKTABLE_NAME\",\n" +
            "\ta2.attname as \"PKCOLUMN_NAME\",\n" +
            "\t'\\E(.*)\\Q'::name as \"FKTABLE_CAT\",\n" +
            "\tn1.nspname as \"FKTABLE_SCHEM\",\n" +
            "\tc1.relname as \"FKTABLE_NAME\",\n" +
            "\ta1.attname as \"FKCOLUMN_NAME\",\n" +
            "\ti::int2 as \"KEY_SEQ\",\n" +
            "\tcase ref.confupdtype\n" +
            "\t\twhen 'c' then 0::int2\n" +
            "\t\twhen 'n' then 2::int2\n" +
            "\t\twhen 'd' then 4::int2\n" +
            "\t\twhen 'r' then 1::int2\n" +
            "\t\telse 3::int2\n" +
            "\tend as \"UPDATE_RULE\",\n" +
            "\tcase ref.confdeltype\n" +
            "\t\twhen 'c' then 0::int2\n" +
            "\t\twhen 'n' then 2::int2\n" +
            "\t\twhen 'd' then 4::int2\n" +
            "\t\twhen 'r' then 1::int2\n" +
            "\t\telse 3::int2\n" +
            "\tend as \"DELETE_RULE\",\n" +
            "\tref.conname as \"FK_NAME\",\n" +
            "\tcn.conname as \"PK_NAME\",\n" +
            "\tcase\n" +
            "\t\twhen ref.condeferrable then\n" +
            "\t\t\tcase\n" +
            "\t\t\twhen ref.condeferred then 5::int2\n" +
            "\t\t\telse 6::int2\n" +
            "\t\t\tend\n" +
            "\t\telse 7::int2\n" +
            "\tend as \"DEFERRABILITY\"\n" +
            " from\n" +
            " ((((((( (select cn.oid, conrelid, conkey, confrelid, confkey,\n" +
            "\t generate_series(array_lower(conkey, 1), array_upper(conkey, 1)) as i,\n" +
            "\t confupdtype, confdeltype, conname,\n" +
            "\t condeferrable, condeferred\n" +
            "  from pg_catalog.pg_constraint cn,\n" +
            "\tpg_catalog.pg_class c,\n" +
            "\tpg_catalog.pg_namespace n\n" +
            "  where contype = 'f' \n" +
            "   and  confrelid = c.oid\n" +
            "   and  relname = E'\\E(.*)\\Q'\n" +
            "   and  n.oid = c.relnamespace\n" +
            "   and  n.nspname = E'\\E(.*)\\Q'\n" +
            " ) ref\n" +
            " inner join pg_catalog.pg_class c1\n" +
            "  on c1.oid = ref.conrelid)\n" +
            " inner join pg_catalog.pg_namespace n1\n" +
            "  on  n1.oid = c1.relnamespace)\n" +
            " inner join pg_catalog.pg_attribute a1\n" +
            "  on  a1.attrelid = c1.oid\n" +
            "  and  a1.attnum = conkey[i])\n" +
            " inner join pg_catalog.pg_class c2\n" +
            "  on  c2.oid = ref.confrelid)\n" +
            " inner join pg_catalog.pg_namespace n2\n" +
            "  on  n2.oid = c2.relnamespace)\n" +
            " inner join pg_catalog.pg_attribute a2\n" +
            "  on  a2.attrelid = c2.oid\n" +
            "  and  a2.attnum = confkey[i])\n" +
            " left outer join pg_catalog.pg_constraint cn\n" +
            "  on cn.conrelid = ref.confrelid\n" +
            "  and cn.contype = 'p')\n" +
            "  order by ref.oid, ref.i\\E";

    private final static Pattern P_ATTRIBUTES = Pattern.compile(QUERY_ATTRIBUTES);
    private final static Pattern P_INDEXED_ATTRIBUTES_1 = Pattern.compile(QUERY_INDEXED_ATTRIBUTES_1);
    private final static Pattern P_INDEXED_ATTRIBUTES_2 = Pattern.compile(QUERY_INDEXED_ATTRIBUTES_2);
    private final static Pattern P_CONSTRAINT_1 = Pattern.compile(QUERY_CONSTRAINT_1);
    private final static Pattern P_CONSTRAINT_2 = Pattern.compile(QUERY_CONSTRAINT_2);

    public MockStatement(QueryProviderImpl provider, String name, StatementDescription description, ThrowingSupplier<Iterator<?>, ProtocolException> op) {
        this.name = name;
        this.provider = provider;
        this.description = description;
        this.op = op;
    }

    public static MockStatement mockFor(QueryProviderImpl provider, Session session, String stmt, String query) {
        if (query.equalsIgnoreCase(QUERY_TABLES))
            return queryMockTables(provider, session, stmt);

        Matcher m;
        if ((m = P_ATTRIBUTES.matcher(query)).find())
            return queryMockAttributes(provider, session, stmt, m);
        if (P_INDEXED_ATTRIBUTES_1.matcher(query).find())
            return queryMockIndexes1(provider, stmt);
        if (P_INDEXED_ATTRIBUTES_2.matcher(query).find())
            return queryMockIndexes2(provider, stmt);
        if (P_CONSTRAINT_1.matcher(query).find())
            return queryMockConstraints(provider, stmt);
        if (P_CONSTRAINT_2.matcher(query).find())
            return queryMockConstraints(provider, stmt);

        return null;
    }

    private static MockStatement queryMockTables(QueryProviderImpl provider, Session session, String stmt) {
        StatementDescription description = new StatementDescription(
                ParametersDescription.EMPTY,
                new RowDescription(ImmutableList.of(
                        new ColumnDescription("relname", TypeUtils.PG_TYPE_NAME),
                        new ColumnDescription("nspname", TypeUtils.PG_TYPE_NAME),
                        new ColumnDescription("relkind", TypeUtils.PG_TYPE_CHAR)
                ))
        );
        return new MockStatement(provider, stmt, description, () -> executeQuery0(session));
    }

    @NotNull
    private static Iterator<Object[]> executeQuery0(Session session) throws NonBreakingException {
        try {
            ImmutableList.Builder<Object[]> b = ImmutableList.builder();
            SpaceRuntimeInfo info = ((IRemoteJSpaceAdmin) session.getSpace().getAdmin()).getRuntimeInfo();
            for (String name : info.m_ClassNames) {
                b.add(new Object[]{name, "public", 'r'});
            }
            return b.build().iterator();
        } catch (RemoteException e) {
            throw new NonBreakingException(ErrorCodes.INTERNAL_ERROR, "failed to get tables");
        }
    }

    private static MockStatement queryMockAttributes(QueryProviderImpl provider, Session session, String stmt, Matcher m) {
        String tableName = m.group(1);
        StatementDescription description = new StatementDescription(
                ParametersDescription.EMPTY,
                new RowDescription(ImmutableList.of(
                        new ColumnDescription("nspname", TypeUtils.PG_TYPE_NAME),
                        new ColumnDescription("relname", TypeUtils.PG_TYPE_NAME),
                        new ColumnDescription("attname", TypeUtils.PG_TYPE_NAME),
                        new ColumnDescription("atttypid", TypeUtils.PG_TYPE_OID),
                        new ColumnDescription("typname", TypeUtils.PG_TYPE_NAME),
                        new ColumnDescription("attnum", TypeUtils.PG_TYPE_INT2),
                        new ColumnDescription("attlen", TypeUtils.PG_TYPE_INT2),
                        new ColumnDescription("atttypmod", TypeUtils.PG_TYPE_INT4),
                        new ColumnDescription("attnotnull", TypeUtils.PG_TYPE_BOOL),
                        new ColumnDescription("relhasrules", TypeUtils.PG_TYPE_BOOL),
                        new ColumnDescription("relkind", TypeUtils.PG_TYPE_CHAR),
                        new ColumnDescription("oid", TypeUtils.PG_TYPE_OID),
                        new ColumnDescription("expr0", TypeUtils.PG_TYPE_TEXT),
                        new ColumnDescription("expr1", TypeUtils.PG_TYPE_OID),
                        new ColumnDescription("typtypmod", TypeUtils.PG_TYPE_INT4),
                        new ColumnDescription("relhasoids", TypeUtils.PG_TYPE_BOOL),
                        new ColumnDescription("expr2", TypeUtils.PG_TYPE_TEXT),
                        new ColumnDescription("relhassubclass", TypeUtils.PG_TYPE_BOOL)
                ))
        );
        return new MockStatement(provider, stmt, description, () -> executeTableQuery(session, tableName));
    }

    private static Iterator<?> executeTableQuery(Session session, String tableName) throws ProtocolException {
        try {
            ImmutableList.Builder<Object[]> b = ImmutableList.builder();
            ISpaceProxy space = session.getSpace();
            SpaceRuntimeInfo info = ((IRemoteJSpaceAdmin) space.getAdmin()).getRuntimeInfo();
            for (String name : info.m_ClassNames) {
                if(!name.equalsIgnoreCase(tableName))
                    continue;
                String fqn = "public." + name;
                int oid = PgOidGenerator.INSTANCE.oid(fqn);
                ITypeDesc typeDesc = SQLUtil.checkTableExistence(name, space);
                short idx = 0;
                for (PropertyInfo property : typeDesc.getProperties()) {
                    SqlTypeName sqlTypeName = GSTable.mapToSqlType(property.getType());
                    PgTypeDescriptor pgType = PgTypeUtils.fromSqlTypeName(sqlTypeName);

                    b.add(new Object[]{
                            "public",
                            name,
                            property.getName(),
                            pgType.getId(),
                            pgType.getName(),
                            ++idx,
                            (short)pgType.getLength(),
                            -1,
                            false,
                            false,
                            'r',
                            oid,
                            null,
                            0,
                            -1,
                            true,
                            "",
                            false
                    });
                }
            }
            return b.build().iterator();
        } catch (RemoteException | SQLException e) {
            throw new NonBreakingException(ErrorCodes.INTERNAL_ERROR, "failed to get attributes");
        }
    }

    private static MockStatement queryMockIndexes1(QueryProviderImpl provider, String stmt) {
        StatementDescription description = new StatementDescription(
                ParametersDescription.EMPTY,
                new RowDescription(ImmutableList.of(
                        new ColumnDescription("attname", TypeUtils.PG_TYPE_NAME),
                        new ColumnDescription("attnum", TypeUtils.PG_TYPE_INT2),
                        new ColumnDescription("relname", TypeUtils.PG_TYPE_NAME),
                        new ColumnDescription("nspname", TypeUtils.PG_TYPE_NAME),
                        new ColumnDescription("relname1", TypeUtils.PG_TYPE_NAME)
                ))
        );
        return new MockStatement(provider, stmt, description, Collections::emptyIterator);
    }

    private static MockStatement queryMockIndexes2(QueryProviderImpl provider, String stmt) {
        StatementDescription description = new StatementDescription(
                ParametersDescription.EMPTY,
                new RowDescription(ImmutableList.of(
                        new ColumnDescription("attname", TypeUtils.PG_TYPE_NAME),
                        new ColumnDescription("attnum", TypeUtils.PG_TYPE_INT2),
                        new ColumnDescription("relname", TypeUtils.PG_TYPE_NAME),
                        new ColumnDescription("nspname", TypeUtils.PG_TYPE_NAME),
                        new ColumnDescription("exp1", TypeUtils.PG_TYPE_UNKNOWN)
                ))
        );
        return new MockStatement(provider, stmt, description, Collections::emptyIterator);
    }

    private static MockStatement queryMockConstraints(QueryProviderImpl provider, String stmt) {
        StatementDescription description = new StatementDescription(
                ParametersDescription.EMPTY,
                new RowDescription(ImmutableList.of(
                        new ColumnDescription("PKTABLE_CAT", TypeUtils.PG_TYPE_NAME),
                        new ColumnDescription("PKTABLE_SCHEM", TypeUtils.PG_TYPE_NAME),
                        new ColumnDescription("PKCOLUMN_NAME", TypeUtils.PG_TYPE_NAME),
                        new ColumnDescription("FKTABLE_CAT", TypeUtils.PG_TYPE_NAME),
                        new ColumnDescription("FKTABLE_SCHEM", TypeUtils.PG_TYPE_NAME),
                        new ColumnDescription("FKTABLE_NAME", TypeUtils.PG_TYPE_NAME),
                        new ColumnDescription("FKCOLUMN_NAME", TypeUtils.PG_TYPE_NAME),
                        new ColumnDescription("KEY_SEQ", TypeUtils.PG_TYPE_INT4),
                        new ColumnDescription("UPDATE_RULE", TypeUtils.PG_TYPE_INT2),
                        new ColumnDescription("DELETE_RULE", TypeUtils.PG_TYPE_INT2),
                        new ColumnDescription("FK_NAME", TypeUtils.PG_TYPE_NAME),
                        new ColumnDescription("PK_NAME", TypeUtils.PG_TYPE_NAME),
                        new ColumnDescription("DEFERRABILITY", TypeUtils.PG_TYPE_INT2)
                ))
        );
        return new MockStatement(provider, stmt, description, Collections::emptyIterator);
    }

    private final String name;
    private final QueryProviderImpl provider;
    private final StatementDescription description;
    private final ThrowingSupplier<Iterator<?>, ProtocolException> op;

    @Override
    public String getName() {
        return name;
    }

    @Override
    public SqlNode getQuery() {
        return null;
    }

    @Override
    public GSOptimizer getOptimizer() {
        return null;
    }

    @Override
    public StatementDescription getDescription() {
        return description;
    }

    @Override
    public void close() throws Exception {
        provider.closeS(name);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public <T> Portal<T> createPortal(String name, int[] fc) {
        return new QueryPortal<T>(provider, name, this, PortalCommand.SELECT, fc, (ThrowingSupplier)op);
    }
}
