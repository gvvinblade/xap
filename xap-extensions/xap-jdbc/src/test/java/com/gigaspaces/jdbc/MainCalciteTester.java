package com.gigaspaces.jdbc;

import com.gigaspaces.jdbc.data.Customer;
import com.gigaspaces.jdbc.data.DataGenerator;
import com.gigaspaces.jdbc.data.Product;
import com.gigaspaces.jdbc.data.Purchase;
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

            execute(statement, String.format("select c.first_name,purchase.productId from %s as c " +
                    "inner join " +
                    "%s as purchase " +
                    "on c.id = purchase.customerId", "\"" + Customer.class.getName() + "\"", "\"" + Purchase.class.getName() + "\""));
//            execute(statement, String.format("SELECT age, email FROM %s where last_name = 'Bb'", "\"" + MyPojo.class.getName() + "\""));
//            execute(statement, String.format("SELECT first_name as first, last_name as last FROM %s where last_name = 'Bb'", "\"" + MyPojo.class.getName() + "\""));
//            execute(statement, String.format("SELECT * FROM %s as T where (T.last_name = 'Bb' AND T.first_name = 'Adam') OR ((T.last_name = 'Cc') or (T.email = 'Adler@msn.com') or (T.age>40))", "\"" + MyPojo.class.getName() + "\""));
//            execute(statement, String.format("SELECT * FROM %s as T where T.last_name = 'Bb' or T.first_name = 'Adam' or T.last_name = 'Cc' or T.email = 'Adler@msn.com' or T.age>=40", "\"" + MyPojo.class.getName() + "\""));
//            execute(statement, "SELECT * FROM com.gigaspaces.jdbc.MyPojo as T where (T.last_name = 'Bb' AND T.first_name = 'Adam') OR ((T.last_name = 'Cc') or (T.email = 'Adler@msn.com') or (T.age>=40))");
//            execute(statement, String.format("SELECT * FROM %s as T where T.last_name = 'Aa' OR T.first_name = 'Adam'", "\"" + MyPojo.class.getName() + "\""));
//            execute(statement, String.format("SELECT * FROM %s as T where T.age <= 40", "\"" + MyPojo.class.getName() + "\""));
//            execute(statement, String.format("EXPLAIN PLAN FOR SELECT * FROM %s ", "\"" + MyPojo.class.getName() + "\""));
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
        DataGenerator.writeProduct(gigaSpace);
        DataGenerator.writePurchase(gigaSpace);
        DataGenerator.writeCustomer(gigaSpace);
        DataGenerator.writeInventory(gigaSpace);
        return gigaSpace;
    }
}
