package com.limin.etltool.database;

import com.google.common.base.Strings;
import com.google.common.base.Suppliers;
import com.limin.etltool.core.OutputReport;
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

    private final Supplier<DataSource> delegate;

    private boolean usePool = false;

    public AbstractDatabase(DatabaseConfiguration configuration) {
        this.configuration = configuration;
        delegate = Suppliers.memoize(() -> {
            BasicDataSource basicDataSource = new BasicDataSource();
            basicDataSource.setDriverClassName(configuration.getDriverClassName());
            basicDataSource.setUrl(constructUri(configuration));
            basicDataSource.setUsername(configuration.getUsername());
            basicDataSource.setPassword(configuration.getPassword());
            return basicDataSource;
        });
        initDriverManager();
    }

    protected void initDriverManager() {
        try {
            Class.forName(configuration.getDriverClassName());
        } catch (ClassNotFoundException e) {
            Exceptions.rethrow(e);
        }
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
            if (usePool) return delegate.get().getConnection();
            return newConnection(null, null);
        } catch (SQLException ex) {
            throw propagate(ex);
        }
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        if (usePool) return delegate.get().getConnection(username, password);
        return newConnection(username, password);
    }

    protected Connection newConnection(String username, String password) throws SQLException {
        if (Strings.isNullOrEmpty(username)) username = configuration.getUsername();
        if (Strings.isNullOrEmpty(password)) password = configuration.getPassword();
        return DriverManager.getConnection(constructUri(configuration), username, password);
    }

    @Override
    public void setPoolConfig(GenericObjectPoolConfig poolConfig) {
        if (delegate.get() instanceof BasicDataSource) {
            BasicDataSource dataSource = (BasicDataSource) delegate.get();
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
        if (delegate.get() instanceof BasicDataSource) {
            try {
                ((BasicDataSource) delegate.get()).close();
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
        return delegate.get().unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return delegate.get().isWrapperFor(iface);
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return delegate.get().getLogWriter();
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        delegate.get().setLogWriter(out);
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        delegate.get().setLoginTimeout(seconds);
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return delegate.get().getLoginTimeout();
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return delegate.get().getParentLogger();
    }

}
