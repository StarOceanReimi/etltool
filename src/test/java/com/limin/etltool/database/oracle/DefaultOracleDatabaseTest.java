package com.limin.etltool.database.oracle;

import com.limin.etltool.core.EtlException;
import com.limin.etltool.database.*;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * <p>
 *
 * </p>
 *
 * @author 邱理 WHRDD-PC104
 * @since 2021/11/5
 */
public class DefaultOracleDatabaseTest {

    @Test
    public void test() {
        DatabaseConfiguration configuration = DatabaseConfiguration.newInstance();
        configuration.setUrl("jdbc:oracle:thin:@192.168.137.27:1521:orcl");
        configuration.setUsername("dangjian");
        configuration.setPassword("K_4uA_F6dhnA_zlB");
        configuration.setDriverClassName("oracle.jdbc.OracleDriver");
        Database database = new DefaultOracleDatabase(configuration);
        try (Connection connection = database.getConnection();
             Statement statement = connection.createStatement()) {
            final ResultSet resultSet = statement.executeQuery("select * from B_DJ_DY where ROWNUM <= 1");
            while (resultSet.next()) {
                System.out.println(resultSet.getString("DY_NAME"));
            }

        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    @Test
    public void testInputOutput() throws EtlException {
        DatabaseConfiguration configuration = DatabaseConfiguration.newInstance();
        configuration.setUrl("jdbc:oracle:thin:@192.168.137.27:1521:orcl");
        configuration.setUsername("dangjian");
        configuration.setPassword("K_4uA_F6dhnA_zlB");
        configuration.setDriverClassName("oracle.jdbc.OracleDriver");
        Database database = new DefaultOracleDatabase(configuration);
        DefaultDatabaseAccessor defaultDatabaseAccessor = new DefaultDatabaseAccessor("select count(*) as \"test\" from b_dj_dy");
        NormalDbInput<Map<String, Object>> input = new NormalDbInput<Map<String, Object>>(database, defaultDatabaseAccessor) {};
        System.out.println(input.readCollection().iterator().next().get("test"));
        System.out.println(input.readCollection());
    }

}