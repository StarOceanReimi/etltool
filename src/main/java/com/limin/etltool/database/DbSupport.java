package com.limin.etltool.database;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.sql.Connection;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/18
 */
public abstract class DbSupport<T> implements AutoCloseable {

    private static Logger log = LoggerFactory.getLogger(DbSupport.class);

    private static AtomicInteger threadCount = new AtomicInteger(0);

    protected int isolationLevel = Connection.TRANSACTION_REPEATABLE_READ;

    protected volatile Connection connection;

    protected DatabaseAccessor accessor;

    protected Class<T> componentType;

    protected Database database;

    @SuppressWarnings("unchecked")
    public DbSupport(
            Class<T> componentType,
            Database database,
            DatabaseAccessor accessor) {
        Objects.requireNonNull(database);
        Objects.requireNonNull(accessor);
        if (componentType == null) {
            Type type = getClass().getGenericSuperclass();
            if (type instanceof ParameterizedType) {
                Type actualType = ((ParameterizedType) type).getActualTypeArguments()[0];
                if(actualType instanceof Class)
                    this.componentType = (Class<T>) actualType;
                else if(actualType instanceof ParameterizedType)
                    this.componentType = (Class<T>) ((ParameterizedType) actualType).getRawType();
            }
        } else {
            this.componentType = componentType;
        }
        this.accessor = accessor;
        this.database = database;
        initializeConnection(database);
    }

    public void keepConnectionAlive() {
        ConnectionAliveChecker.getInstance().register("DbSupport-" + threadCount.incrementAndGet(), connection, null);
    }

    public void setIsolationLevel(int level) {
        isolationLevel = level;
    }

    protected void initializeConnection(Database database) {
        connection = database.getConnection();
    }

    @Override
    public void close() throws Exception {
        if (connection != null && !connection.isClosed())
            connection.close();
    }

}
