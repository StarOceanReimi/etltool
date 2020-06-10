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

import java.sql.*;
import java.util.Arrays;
import java.util.List;
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

    private Connection genConnection() {
        try {
            Class.forName(configuration.getDriverClassName());
        } catch (ClassNotFoundException e) {
            rethrow(e);
        }

        String url = configuration.getUrl();

        if (!MapUtils.isEmpty(configuration.getAttributes())) {
            if(!url.contains("?"))
                url += "?" + toQueryString(wrapToQueryStringMap(configuration.getAttributes()));
            else
                url += "&" + toQueryString(wrapToQueryStringMap(configuration.getAttributes()));
        }

        log.info("CONNECTION URL: {}", url);

        try {
            return DriverManager.getConnection(url, configuration.getUsername(), configuration.getPassword());
        } catch (SQLException e) {
            throw propagate(e);
        }
    }

    @Override
    public Connection getConnection() {

        if(pool == null || pool.isClosed())
            return genConnection();
        try {
            return new PooledConnection(pool.borrowObject(), pool);
        } catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public void setPoolConfig(GenericObjectPoolConfig poolConfig) {
        if(pool != null) pool.close();
        pool = createPool(poolConfig);
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
                    if(set.next()) result = set.getInt(1) == 1;
                    set.close();
                    statement.close();
                    return result;
                } catch (SQLException e) {
                    return false;
                }
            }

            @Override
            public Connection create() {
                return getConnection();
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
    public boolean createTable(String table, String tableComment, List<ColumnDefinition> defs, ColumnDefinition.Index[] indices) {
        if(indices == null)
            return createTable(table, tableComment, defs);
        String columnDefs = defs.stream().map(Object::toString).collect(Collectors.joining(","));
        String comment = ofNullable(tableComment).map(c -> "COMMENT '" + c + "'").orElse("");
        String indexOption = Arrays.stream(indices).map(idx -> "index " + idx.toString())
                .collect(Collectors.joining(","));
        String body = columnDefs + (Strings.isNullOrEmpty(indexOption) ? "" : "," + indexOption);
        String ddl = TemplateUtils.logFormat(COMMON_CREATE_TABLE_TPL, table, body, comment);
        log.info("DDL: {}", ddl);
        return executeSQL(ddl);
    }

    @Override
    public boolean executeSQL(String ddl) {

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
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        DatabaseConfiguration configuration = new DatabaseConfiguration();
        val id = ColumnDefinition.builder()
                .name("id")
                .type(BIGINT(20))
                .build();

        val name = ColumnDefinition.builder()
                .name("name")
                .type(VARCHAR(20))
                .build();
        new DefaultMySqlDatabase(configuration).createTable("test", "测试一下",
                Arrays.asList(id, name));

    }
}
