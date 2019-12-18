package com.limin.etltool.database.mysql;

import com.limin.etltool.database.Database;
import com.limin.etltool.database.DatabaseConfiguration;
import org.apache.commons.collections.MapUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import static com.limin.etltool.util.Exceptions.propagate;
import static com.limin.etltool.util.Exceptions.rethrow;
import static com.limin.etltool.util.QueryStringUtils.toQueryString;
import static com.limin.etltool.util.QueryStringUtils.wrapToQueryStringMap;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/18
 */
public class DefaultMySqlDatabase implements Database {

    private final DatabaseConfiguration configuration;

    public DefaultMySqlDatabase(DatabaseConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    public Connection getConnection() {

        try {
            Class.forName(configuration.getDriverClassName());
        } catch (ClassNotFoundException e) {
            rethrow(e);
        }

        String url = configuration.getUrl();

        if (!MapUtils.isEmpty(configuration.getAttributes())) {
            url += "?" + toQueryString(wrapToQueryStringMap(configuration.getAttributes()));
        }

        try {
            return DriverManager.getConnection(url, configuration.getUsername(), configuration.getPassword());
        } catch (SQLException e) {
            throw propagate(e);
        }
    }
}
