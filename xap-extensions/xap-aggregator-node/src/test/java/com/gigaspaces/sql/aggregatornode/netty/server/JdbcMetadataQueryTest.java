package com.gigaspaces.sql.aggregatornode.netty.server;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Statement;

public class JdbcMetadataQueryTest extends AbstractServerTest {
    private static final String SELECT_NAMESPACES = "SELECT nspname AS TABLE_SCHEM, NULL AS TABLE_CATALOG FROM pg_catalog.pg_namespace  WHERE nspname <> 'pg_toast' AND (nspname !~ '^pg_temp_'  OR nspname = (pg_catalog.current_schemas(true))[1]) AND (nspname !~ '^pg_toast_temp_'  OR nspname = replace((pg_catalog.current_schemas(true))[1], 'pg_temp_', 'pg_toast_temp_'))  ORDER BY TABLE_SCHEM";
    private static final String SELECT_TABLES = "SELECT NULL AS TABLE_CAT, n.nspname AS TABLE_SCHEM, c.relname AS TABLE_NAME,  CASE n.nspname ~ '^pg_' OR n.nspname = 'information_schema'  WHEN true THEN CASE  WHEN n.nspname = 'pg_catalog' OR n.nspname = 'information_schema' THEN CASE c.relkind   WHEN 'r' THEN 'SYSTEM TABLE'   WHEN 'v' THEN 'SYSTEM VIEW'   WHEN 'i' THEN 'SYSTEM INDEX'   ELSE NULL   END  WHEN n.nspname = 'pg_toast' THEN CASE c.relkind   WHEN 'r' THEN 'SYSTEM TOAST TABLE'   WHEN 'i' THEN 'SYSTEM TOAST INDEX'   ELSE NULL   END  ELSE CASE c.relkind   WHEN 'r' THEN 'TEMPORARY TABLE'   WHEN 'p' THEN 'TEMPORARY TABLE'   WHEN 'i' THEN 'TEMPORARY INDEX'   WHEN 'S' THEN 'TEMPORARY SEQUENCE'   WHEN 'v' THEN 'TEMPORARY VIEW'   ELSE NULL   END  END  WHEN false THEN CASE c.relkind  WHEN 'r' THEN 'TABLE'  WHEN 'p' THEN 'PARTITIONED TABLE'  WHEN 'i' THEN 'INDEX'  WHEN 'S' THEN 'SEQUENCE'  WHEN 'v' THEN 'VIEW'  WHEN 'c' THEN 'TYPE'  WHEN 'f' THEN 'FOREIGN TABLE'  WHEN 'm' THEN 'MATERIALIZED VIEW'  ELSE NULL  END  ELSE NULL  END  AS TABLE_TYPE, d.description AS REMARKS,  '' as TYPE_CAT, '' as TYPE_SCHEM, '' as TYPE_NAME, '' AS SELF_REFERENCING_COL_NAME, '' AS REF_GENERATION  FROM pg_catalog.pg_namespace n, pg_catalog.pg_class c  LEFT JOIN pg_catalog.pg_description d ON (c.oid = d.objoid AND d.objsubid = 0)  LEFT JOIN pg_catalog.pg_class dc ON (d.classoid=dc.oid AND dc.relname='pg_class')  LEFT JOIN pg_catalog.pg_namespace dn ON (dn.oid=dc.relnamespace AND dn.nspname='pg_catalog')  WHERE c.relnamespace = n.oid  AND n.nspname LIKE 'public' AND (false  OR ( c.relkind = 'f' )  OR ( c.relkind = 'm' )  OR ( c.relkind = 'p' AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' )  OR ( c.relkind = 'r' AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' )  OR ( c.relkind = 'v' AND n.nspname <> 'pg_catalog' AND n.nspname <> 'information_schema' ) )  ORDER BY TABLE_TYPE,TABLE_SCHEM,TABLE_NAME";
    private static final String SELECT_ATTRIBUTES = "SELECT c.oid, a.attnum, a.attname, c.relname, n.nspname, a.attnotnull OR (t.typtype = 'd' AND t.typnotnull), a.attidentity != '' OR pg_catalog.pg_get_expr(d.adbin, d.adrelid) LIKE '%nextval(%' FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON (c.relnamespace = n.oid) JOIN pg_catalog.pg_attribute a ON (c.oid = a.attrelid) JOIN pg_catalog.pg_type t ON (a.atttypid = t.oid) LEFT JOIN pg_catalog.pg_attrdef d ON (d.adrelid = a.attrelid AND d.adnum = a.attnum) JOIN (SELECT 2615 AS oid , 2 AS attnum) vals ON (c.oid = vals.oid AND a.attnum = vals.attnum)";

    @Disabled("SqlParseException: POSIX like match operators are unsupported")
    @Test
    public void testSelectNamespaces() throws Exception {
        checkQuery(SELECT_NAMESPACES);
    }

    @Disabled("SqlParseException: POSIX like match operators are unsupported")
    @Test
    public void testSelectTables() throws Exception {
        checkQuery(SELECT_TABLES);
    }

    @Disabled("SqlValidatorException: cannot look up pg_get_expr function")
    @Test
    public void testSelectAttributes() throws Exception {
        checkQuery(SELECT_ATTRIBUTES);
    }

    private void checkQuery(String query) throws Exception {
        try (Connection connection = connect(true)) {
            Statement statement = connection.createStatement();
            statement.execute(query);
        }
    }
}
