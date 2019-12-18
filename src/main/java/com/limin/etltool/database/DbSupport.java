package com.limin.etltool.database;

import com.google.common.reflect.TypeResolver;
import com.limin.etltool.util.Exceptions;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.sql.Connection;
import java.util.Objects;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/18
 */
public abstract class DbSupport<T> implements AutoCloseable {

    protected Connection connection;

    protected DatabaseAccessor accessor;

    protected Class<T> componentType;

    private static TypeResolver typeResolver = new TypeResolver();

    @SuppressWarnings("unchecked")
    public DbSupport(
            Class<T> componentType,
            Database database,
            DatabaseAccessor accessor) {
        Objects.requireNonNull(database);
        Objects.requireNonNull(accessor);
        if(componentType == null) {
            Type resolvedType = typeResolver.resolveType(getClass().getGenericSuperclass());
            if(resolvedType instanceof ParameterizedType) {
                Type type = ((ParameterizedType) resolvedType).getActualTypeArguments()[0];
                if(type instanceof Class)
                    this.componentType = (Class<T>) type;
            }
        } else {
            this.componentType = componentType;
        }
        this.accessor = accessor;
        initializeConnection(database);
    }

    protected void initializeConnection(Database database) {
        connection = database.getConnection();
    }

    @Override
    public void close() throws Exception {
        if(connection != null && !connection.isClosed())
            connection.close();
    }
}
