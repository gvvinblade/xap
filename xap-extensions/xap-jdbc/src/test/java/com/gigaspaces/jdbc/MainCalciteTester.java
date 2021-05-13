package com.gigaspaces.jdbc;

import org.openspaces.core.GigaSpace;
import org.openspaces.core.GigaSpaceConfigurer;
import org.openspaces.core.space.AbstractSpaceConfigurer;
import org.openspaces.core.space.EmbeddedSpaceConfigurer;
import org.openspaces.core.space.SpaceProxyConfigurer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Properties;

public class MainCalciteTester {
    public static void main(String[] args) throws SQLException, ParseException {
        GigaSpace space = createAndFillSpace(true, true);

        Properties properties = new Properties();
        properties.put("com.gs.embeddedQP.enabled", "true");

        try {
            Class.forName("com.j_spaces.jdbc.driver.GDriver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        try (Connection connection = DriverManager.getConnection("jdbc:gigaspaces:v3://localhost:4174/" + space.getSpaceName(), properties)) {
            Statement statement = connection.createStatement();

            execute(statement, String.format("SELECT first_name FROM (SELECT id, age, email, first_name, last_name FROM %s) where (last_name = 'Bb' AND first_name = 'Adam') OR ((last_name = 'Cc') or (email = 'Adler@msn.com') or (age>=40))", "\"" + MyPojo.class.getName() + "\""));
        }
    }

    private static void execute(Statement statement, String sql) throws SQLException {
        System.out.println();
        System.out.println("Executing: " + sql);
        ResultSet res = statement.executeQuery(sql);
        DumpUtils.dump(res);

    }

    private static GigaSpace createAndFillSpace(boolean newDriver, boolean embedded) throws ParseException {
        String spaceName = "demo" + (newDriver ? "new" : "old");
        AbstractSpaceConfigurer configurer = embedded ? new EmbeddedSpaceConfigurer(spaceName)
                .addProperty("space-config.QueryProcessor.datetime_format", "yyyy-MM-dd HH:mm:ss.SSS")
//                .tieredStorage(new TieredStorageConfigurer().addTable(new TieredStorageTableConfig().setName(MyPojo.class.getName()).setCriteria("age > 20")))
                : new SpaceProxyConfigurer(spaceName);

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss.SSS");
        GigaSpace gigaSpace = new GigaSpaceConfigurer(configurer).gigaSpace();
        if (embedded || gigaSpace.count(null) == 0) {
            java.util.Date date1 = simpleDateFormat.parse("10/09/2001 05:20:00.231");
            java.util.Date date2 = simpleDateFormat.parse("11/09/2001 10:20:00.250");
            java.util.Date date3 = simpleDateFormat.parse("12/09/2001 15:20:00.100");
            java.util.Date date4 = simpleDateFormat.parse("13/09/2001 20:20:00.300");
            gigaSpace.write(new MyPojo("Adler Aa", 20, "Israel", date1, new Time(date1.getTime()), new Timestamp(date1.getTime())));
            gigaSpace.write(new MyPojo("Adam Bb", 30, "Israel", date2, new Time(date2.getTime()), new Timestamp(date2.getTime())));
            gigaSpace.write(new MyPojo("Eve Cc", 35, "UK", date3, new Time(date3.getTime()), new Timestamp(date3.getTime())));
            gigaSpace.write(new MyPojo("NoCountry Dd", 40, null, date4, new Time(date4.getTime()), new Timestamp(date4.getTime())));
        }
        return gigaSpace;
    }
}
