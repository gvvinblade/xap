package com.gigaspaces.sql.datagateway.netty.server;

import com.gigaspaces.sql.datagateway.netty.data.Student;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.openspaces.core.GigaSpaceConfigurer;
import org.openspaces.core.space.EmbeddedSpaceConfigurer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class RepeatedColumnsTest extends AbstractServerTest {
    @BeforeAll
    static void setUp() {
        gigaSpace = new GigaSpaceConfigurer(
            new EmbeddedSpaceConfigurer(SPACE_NAME)
                .addProperty("space-config.QueryProcessor.datetime_format", "yyyy-MM-dd HH:mm:ss.SSS")
        ).gigaSpace();

        gigaSpace.write(new Student("1", "Kevin", 21));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testRepeatedColumns0(boolean simple) throws Exception {
        try (Connection conn = connect(simple)) {
            final String qry = String.format("" +
                "select name, age from ( " +
                "select age, name, id from %1$s ) " +
                "order by id " +
                "", "\"" + Student.class.getName() + "\"");

            final PreparedStatement statement = conn.prepareStatement(qry);
            assertTrue(statement.execute());
            ResultSet res = statement.getResultSet();
            String expected = "" +
"| name  | age |\n" +
"| ----- | --- |\n" +
"| Kevin | 21  |\n";
            DumpUtils.checkResult(res, expected);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testRepeatedColumns1(boolean simple) throws Exception {
        try (Connection conn = connect(simple)) {
            final String qry = String.format("" +
                "select * from %1$s a " +
                "inner join " +
                "%1$s b on a.id = b.id " +
                "", "\"" + Student.class.getName() + "\"");

            final PreparedStatement statement = conn.prepareStatement(qry);
            assertTrue(statement.execute());
            ResultSet res = statement.getResultSet();
            String expected = "" +
"| age | id | name  | age | id | name  |\n" +
"| --- | -- | ----- | --- | -- | ----- |\n" +
"| 21  | 1  | Kevin | 21  | 1  | Kevin |\n";
            DumpUtils.checkResult(res, expected);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testRepeatedColumns2(boolean simple) throws Exception {
        try (Connection conn = connect(simple)) {
            final String qry = String.format("" +
                "select * from ( " +
                "select *, 22 as userCol, 48 from (" +
                "select * from %1$s as a " +
                "inner join " +
                "%1$s as b on a.id = b.id " +
                "inner join " +
                "%1$s as c on b.id = c.id )) " +
                "", "\"" + Student.class.getName() + "\"");

            final PreparedStatement statement = conn.prepareStatement(qry);
            assertTrue(statement.execute());
            ResultSet res = statement.getResultSet();
            String expected = "" +
"| age | id | name  | age | id | name  | age | id | name  | userCol | EXPR$10 |\n" +
"| --- | -- | ----- | --- | -- | ----- | --- | -- | ----- | ------- | ------- |\n" +
"| 21  | 1  | Kevin | 21  | 1  | Kevin | 21  | 1  | Kevin | 22      | 48      |\n";
            DumpUtils.checkResult(res, expected);
        }
    }

    @Disabled("unsupported UNION operation")
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testRepeatedColumns3(boolean simple) throws Exception {
        try (Connection conn = connect(simple)) {
            final String qry = String.format("" +
                "select * from %1$s a " +
                "inner join " +
                "%1$s b on a.id = b.id " +
                "union all " +
                "select a.age age0, a.id id0, a.name name0, b.age age1, b.id id1, b.name name1 " +
                "from %1$s a " +
                "inner join " +
                "%1$s b on a.id = b.id " +
                "", "\"" + Student.class.getName() + "\"");

            final PreparedStatement statement = conn.prepareStatement(qry);
            assertTrue(statement.execute());
            ResultSet res = statement.getResultSet();
            String expected = "" +
"| age | id | name  | age | id | name  |\n" +
"| --- | -- | ----- | --- | -- | ----- |\n" +
"| 21  | 1  | Kevin | 21  | 1  | Kevin |\n" +
"| 21  | 1  | Kevin | 21  | 1  | Kevin |\n";
            DumpUtils.checkResult(res, expected);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testRepeatedColumns4(boolean simple) throws Exception {
        try (Connection conn = connect(simple)) {
            final String qry = String.format("" +
                "with cte as (select * from ( " +
                "select *, 22 as userCol, 48 from (" +
                "select * from %1$s as a " +
                "inner join " +
                "%1$s as b on a.id = b.id " +
                "inner join " +
                "%1$s as c on b.id = c.id ))) " +
                "select * from cte " +
                "", "\"" + Student.class.getName() + "\"");

            final PreparedStatement statement = conn.prepareStatement(qry);
            assertTrue(statement.execute());
            ResultSet res = statement.getResultSet();
            String expected = "" +
"| age | id | name  | age | id | name  | age | id | name  | userCol | EXPR$10 |\n" +
"| --- | -- | ----- | --- | -- | ----- | --- | -- | ----- | ------- | ------- |\n" +
"| 21  | 1  | Kevin | 21  | 1  | Kevin | 21  | 1  | Kevin | 22      | 48      |\n";
            DumpUtils.checkResult(res, expected);
        }
    }
}
