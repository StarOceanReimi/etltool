package com.limin.etltool.database.mysql;

import com.limin.etltool.database.Database;
import com.limin.etltool.database.DatabaseConfiguration;
import com.limin.etltool.database.StatementCallback;
import com.limin.etltool.util.TemplateUtils;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.*;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static com.limin.etltool.database.mysql.ColumnDefinition.BIGINT;
import static com.limin.etltool.database.mysql.ColumnDefinition.VARCHAR;
import static com.limin.etltool.util.Exceptions.propagate;
import static com.limin.etltool.util.Exceptions.rethrow;
import static com.limin.etltool.util.QueryStringUtils.toQueryString;
import static com.limin.etltool.util.QueryStringUtils.wrapToQueryStringMap;
import static java.util.Optional.ofNullable;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/18
 */
@Slf4j
public class DefaultMySqlDatabase implements Database {

    private final DatabaseConfiguration configuration;

    private final DataSource delegate;

    public DefaultMySqlDatabase(DatabaseConfiguration configuration) {
        this.configuration = configuration;
        BasicDataSource basicDataSource = new BasicDataSource();
        basicDataSource.setDriverClassName(configuration.getDriverClassName());
        basicDataSource.setUrl(constructUri(configuration));
        basicDataSource.setUsername(configuration.getUsername());
        basicDataSource.setPassword(configuration.getPassword());
        delegate = basicDataSource;
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
            return delegate.getConnection();
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
        return delegate.getConnection(username, password);
    }

    @Override
    public void setPoolConfig(GenericObjectPoolConfig poolConfig) {
        if (delegate instanceof BasicDataSource) {
            BasicDataSource dataSource = (BasicDataSource) delegate;
            dataSource.setMaxIdle(poolConfig.getMaxIdle());
            dataSource.setMinIdle(poolConfig.getMinIdle());
            dataSource.setMaxWaitMillis(poolConfig.getMaxWaitMillis());
            dataSource.setMaxTotal(poolConfig.getMaxTotal());
        }
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
    public boolean createTable(String table, String tableComment, List<ColumnDefinition> defs) {
        String columnDefs = defs.stream().map(Object::toString).collect(Collectors.joining(","));
        String comment = ofNullable(tableComment).map(c -> "COMMENT '" + c + "'").orElse("");
        String ddl = TemplateUtils.logFormat(COMMON_CREATE_TABLE_TPL, table, columnDefs, comment);
        return executeSQL(ddl);
    }

    private static final String COMMON_CREATE_TABLE_TPL = "CREATE TABLE {} ({}) {}";

    @Override
    public boolean createTable(String table,
                               String tableComment,
                               List<ColumnDefinition> defs,
                               String[] primaryKeys,
                               ColumnDefinition.Index[] indices) {
        if (indices == null && primaryKeys == null)
            return createTable(table, tableComment, defs);
        String columnDefs = defs.stream().peek(def -> def.setPrimaryKey(false)).map(Object::toString).collect(Collectors.joining(","));
        String comment = ofNullable(tableComment).map(c -> "COMMENT '" + c + "'").orElse("");
        String indexOption = ofNullable(indices).map(i -> Arrays.stream(i).map(idx -> "index " + idx.toString())
                .collect(Collectors.joining(","))).map(s -> "," + s).orElse("");
        String primaryOption = primaryKeys != null && primaryKeys.length > 0 ?
                " ,PRIMARY KEY (" + String.join(",", primaryKeys) + ")" : "";
        String body = columnDefs + primaryOption + indexOption;
        String ddl = TemplateUtils.logFormat(COMMON_CREATE_TABLE_TPL, table, body, comment);
        return executeSQL(ddl);
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
    public void optimizeForBatchWriting() {
        getConfiguration().attribute("rewriteBatchedStatements", true);
    }

    @Override
    public DatabaseConfiguration getConfiguration() {
        return configuration;
    }

    public static void main(String[] args) {
        DatabaseConfiguration configuration = new DatabaseConfiguration();
//        val id = ColumnDefinition.builder()
//                .name("id")
//                .type(BIGINT(20))
//                .build();
//        val name = ColumnDefinition.builder()
//                .name("name")
//                .type(VARCHAR(20))
//                .build();
//        new DefaultMySqlDatabase(configuration).createTable("test", "测试一下", Arrays.asList(id, name));

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
