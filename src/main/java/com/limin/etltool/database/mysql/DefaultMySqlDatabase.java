package com.limin.etltool.database.mysql;

import com.google.common.base.Strings;
import com.limin.etltool.database.Database;
import com.limin.etltool.database.DatabaseConfiguration;
import com.limin.etltool.database.PooledConnection;
import com.limin.etltool.util.TemplateUtils;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

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

    public DefaultMySqlDatabase(DatabaseConfiguration configuration) {
        this.configuration = configuration;
    }

    private GenericObjectPool<Connection> pool;

    private GenericObjectPoolConfig poolConfig;

    private Connection genConnection() {
        try {
            Class.forName(configuration.getDriverClassName());
        } catch (ClassNotFoundException e) {
            rethrow(e);
        }

        String url = configuration.getUrl();

        if (!MapUtils.isEmpty(configuration.getAttributes())) {
            if (!url.contains("?"))
                url += "?" + toQueryString(wrapToQueryStringMap(configuration.getAttributes()));
            else
                url += "&" + toQueryString(wrapToQueryStringMap(configuration.getAttributes()));
        }

        log.debug("CONNECTION URL: {}", url);

        try {
            return DriverManager.getConnection(url, configuration.getUsername(), configuration.getPassword());
        } catch (SQLException e) {
            throw propagate(e);
        }
    }

    @Override
    public Connection getConnection() {

        if(pool == null) return genConnection();
        if(pool.isClosed()) pool = createPool(poolConfig);
        try {
            return new PooledConnection(pool.borrowObject(), pool);
        } catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return getConnection();
    }

    @Override
    public void setPoolConfig(GenericObjectPoolConfig poolConfig) {
        if (pool != null) pool.close();
        this.poolConfig = poolConfig;
        pool = createPool(poolConfig);
    }

    @Override
    public void shutdownPool() {
        pool.close();
    }

    private GenericObjectPool<Connection> createPool(GenericObjectPoolConfig poolConfig) {

        PooledObjectFactory<Connection> factory = new BasePooledObjectFactory<Connection>() {

            @Override
            public void destroyObject(PooledObject<Connection> p) throws Exception {
                p.getObject().close();
            }

            @Override
            public boolean validateObject(PooledObject<Connection> p) {
                try {
                    Statement statement = p.getObject().createStatement();
                    ResultSet set = statement.executeQuery("select 1");
                    boolean result = false;
                    if (set.next()) result = set.getInt(1) == 1;
                    set.close();
                    statement.close();
                    return result;
                } catch (SQLException e) {
                    log.error("connection validation failed. ", e);
                    return false;
                }
            }

            @Override
            public Connection create() {
                return genConnection();
            }

            @Override
            public PooledObject<Connection> wrap(Connection obj) {
                return new DefaultPooledObject<>(obj);
            }
        };

        return new GenericObjectPool<>(factory, poolConfig);
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
    public boolean executeSQL(String ddl) {
        log.debug("EXECUTING SQL: {}", ddl);
        Connection connection = getConnection();
        boolean result;
        try {
            Statement statement = connection.createStatement();
            result = statement.execute(ddl);
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
    public void optimizeForBatchWriting() {
        getConfiguration().attribute("rewriteBatchedStatements", true);
    }

    @Override
    public DatabaseConfiguration getConfiguration() {
        return configuration;
    }

    public static void main(String[] args) {
        DatabaseConfiguration configuration = new DatabaseConfiguration();
        val id = ColumnDefinition.builder()
                .name("id")
                .type(BIGINT(20))
                .build();

        val name = ColumnDefinition.builder()
                .name("name")
                .type(VARCHAR(20))
                .build();
        new DefaultMySqlDatabase(configuration).createTable("test", "测试一下", Arrays.asList(id, name));

    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        try {
            // This works for classes that aren't actually wrapping anything
            return iface.cast(this);
        } catch (ClassCastException cce) {
            throw new SQLException("Unable to unwrap to " + iface.toString());
        }
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isAssignableFrom(getClass());
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return DriverManager.getLogWriter();
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        DriverManager.setLogWriter(out);
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        DriverManager.setLoginTimeout(seconds);
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return DriverManager.getLoginTimeout();
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return Logger.getLogger("MySqlDatabaseGlobal");
    }
}
