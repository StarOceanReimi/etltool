package com.limin.etltool.runner;

import com.limin.etltool.database.DatabaseConfiguration;
import com.limin.etltool.database.mysql.DefaultMySqlDatabase;
import com.limin.etltool.database.util.DatabaseUtils;

import java.sql.*;
import java.util.Map;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/27
 */
public class Main {

    public static void main(String[] args) throws SQLException {
        DatabaseConfiguration configuration = DatabaseConfiguration.newInstance();
        configuration.setUrl(String.format("jdbc:mysql://%s:3306/%s?useSSL=false", args[0], args[1]));
        configuration.setDriverClassName("com.mysql.jdbc.Driver");
        configuration.setUsername(System.getProperty("username"));
        configuration.setPassword(System.getProperty("password"));
        Connection conn = new DefaultMySqlDatabase(configuration).getConnection();
        Statement statement = conn.createStatement();
        ResultSet set = statement.executeQuery(args[2]);
        while (set.next()) {
            System.out.println(DatabaseUtils.readObjectFromResultSet(set, Map.class));
        }
        set.close();
        statement.close();
        conn.close();
    }
}
