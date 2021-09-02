package com.limin.etltool.database;

import com.google.common.base.Strings;
import com.google.common.base.Suppliers;
import com.limin.etltool.util.Exceptions;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.*;
import java.util.function.Supplier;
import java.util.logging.Logger;

import static com.limin.etltool.util.Exceptions.propagate;
import static com.limin.etltool.util.Exceptions.rethrow;
import static com.limin.etltool.util.QueryStringUtils.toQueryString;
import static com.limin.etltool.util.QueryStringUtils.wrapToQueryStringMap;

/**
 * <p>
 *
 * </p>
 *
 * @author 邱理 WHRDD-PC104
 * @since 2021/5/11
 */
@Slf4j
public abstract class AbstractDatabase implements Database {

    private final DatabaseConfiguration configuration;

    private final DataSource delegate;

    private boolean usePool = false;

    private final Supplier<String> configuredUrl;

    public AbstractDatabase(DatabaseConfiguration configuration) {
        this.configuration = configuration;
        configuredUrl = Suppliers.memoize(() -> constructUri(configuration));
        BasicDataSource basicDataSource = new BasicDataSource();
        basicDataSource.setDriverClassName(configuration.getDriverClassName());
        basicDataSource.setUrl(configuredUrl.get());
        basicDataSource.setUsername(configuration.getUsername());
        basicDataSource.setPassword(configuration.getPassword());
        initDriverManager();
        delegate = basicDataSource;
    }

    protected void initDriverManager() {
        try {
            Class.forName(configuration.getDriverClassName());
        } catch (ClassNotFoundException e) {
            Exceptions.rethrow(e);
        }
    }

    public static void main(String[] args) {

    }

    private static String constructUri(DatabaseConfiguration configuration) {
        String url = configuration.getUrl();
        if (!MapUtils.isEmpty(configuration.getAttributes())) {
            if (!url.contains("?"))
                url += "?" + toQueryString(wrapToQueryStringMap(configuration.getAttributes()));
            else
                url += "&" + toQueryString(wrapToQueryStringMap(configuration.getAttributes()));
        }
        log.debug("CONNECTION URL: {}", url);
        return url;
    }


    @Override
    public Connection getConnection() {
        try {
            if (usePool) return delegate.getConnection();
            return newConnection(null, null);
        } catch (SQLException ex) {
            throw propagate(ex);
        }
//        if (pool == null) return genConnection();
//        if (pool.isClosed()) pool = createPool(poolConfig);
//        try {
//            return new PooledConnection(pool.borrowObject(), pool);
//        } catch (Exception e) {
//            throw propagate(e);
//        }
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        if (usePool) return delegate.getConnection(username, password);
        return newConnection(username, password);
    }

    protected Connection newConnection(String username, String password) throws SQLException {
        if (Strings.isNullOrEmpty(username)) username = configuration.getUsername();
        if (Strings.isNullOrEmpty(password)) password = configuration.getPassword();
        return DriverManager.getConnection(configuredUrl.get(), username, password);
    }

    @Override
    public void setPoolConfig(GenericObjectPoolConfig poolConfig) {
        if (delegate instanceof BasicDataSource) {
            BasicDataSource dataSource = (BasicDataSource) delegate;
            dataSource.setMaxIdle(poolConfig.getMaxIdle());
            dataSource.setMinIdle(poolConfig.getMinIdle());
            dataSource.setMaxWaitMillis(poolConfig.getMaxWaitMillis());
            dataSource.setMaxTotal(poolConfig.getMaxTotal());
            usePool = true;
        }
    }

    @Override
    public void usePool(boolean enable) {
        this.usePool = enable;
    }

    @Override
    public void shutdownPool() {
        if (delegate instanceof BasicDataSource) {
            try {
                ((BasicDataSource) delegate).close();
            } catch (SQLException ex) {
                log.error("shutdown pool error: ", ex);
            }
        }
    }

    @Override
    public boolean executeSQL(String sql, StatementCallback callback) {
        log.debug("EXECUTING SQL: {}", sql);
        Connection connection = getConnection();
        boolean result;
        try {
            Statement statement = connection.createStatement();
            result = statement.execute(sql);
            if (callback != null) callback.doWithStatement(result, statement);
        } catch (SQLException ex) {
            result = false;
            rethrow(ex);
        } finally {
            try {
                connection.close();
            } catch (SQLException e) {
                rethrow(e);
            }
        }
        return result;
    }

    @Override
    public boolean executeSQL(String ddl) {
        return executeSQL(ddl, null);
    }

    @Override
    public DatabaseConfiguration getConfiguration() {
        return configuration;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return delegate.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return delegate.isWrapperFor(iface);
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return delegate.getLogWriter();
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        delegate.setLogWriter(out);
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        delegate.setLoginTimeout(seconds);
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return delegate.getLoginTimeout();
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return delegate.getParentLogger();
    }

}
