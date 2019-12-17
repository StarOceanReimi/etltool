package com.limin.etltool.database;

import com.limin.etltool.util.Exceptions;
import org.apache.commons.collections.MapUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import static com.limin.etltool.util.Exceptions.propagate;
import static com.limin.etltool.util.QueryStringUtils.toQueryString;
import static com.limin.etltool.util.QueryStringUtils.wrapToQueryStringMap;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/17
 */
public abstract class AbstractJdbcDatabaseSource implements DatabaseSource {

    protected final DatabaseConfiguration configuration;

    public AbstractJdbcDatabaseSource(DatabaseConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    public Connection getConnection() {

        try {
            Class.forName(configuration.getDriverClassName());
        } catch (ClassNotFoundException e) {
            Exceptions.rethrow(e);
        }
        String url = configuration.getUrl();

        if(!MapUtils.isEmpty(configuration.getAttributes())) {
            url += "?" + toQueryString(wrapToQueryStringMap(configuration.getAttributes()));
        }

        try {
            return DriverManager.getConnection(url, configuration.getUsername(), configuration.getPassword());
        } catch (SQLException e) {
            throw propagate(e);
        }
    }
}
