package com.limin.etltool.database;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.limin.etltool.core.Batch;
import com.limin.etltool.database.util.DatabaseUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import static com.limin.etltool.util.Exceptions.propagate;
import static com.limin.etltool.util.Exceptions.rethrow;
import static java.util.Optional.ofNullable;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/17
 */
class DatabaseBatchObject<T> implements Batch<T> {

    private final ResultSet resultSet;

    private final int batchSize;

    private boolean hasNext;

    private final Class<T> componentType;

    private AtomicReference<T> initTestObject;

    @SuppressWarnings("unchecked")
    DatabaseBatchObject(ResultSet resultSet, int batchSize, Class<T> componentType) {
        Objects.requireNonNull(resultSet);
        Preconditions.checkArgument(batchSize > 0, "batchSize must be positive");
        this.resultSet = resultSet;
        this.batchSize = batchSize;
        this.initTestObject = new AtomicReference<>();
        this.componentType = ofNullable(componentType).orElse((Class<T>) Map.class);

        try {
            hasNext = resultSet.next();
            if(hasNext) {
                T object = DatabaseUtils.readObjectFromResultSet(resultSet, this.componentType);
                initTestObject.set(object);
            }
        } catch (SQLException e) {
            rethrow(e);
        }

    }

    @Override
    public boolean hasMore() {
        return hasNext;
    }

    @Override
    public List<T> getMore() {

        List<T> result = Lists.newLinkedList();
        T initObject = initTestObject.getAndSet(null);
        if(initObject != null) result.add(initObject);
        try {
            while (true) {
                if(result.size() == batchSize) break;
                hasNext = resultSet.next();
                if(!hasNext) break;
                result.add(DatabaseUtils.readObjectFromResultSet(resultSet, componentType));
            }
        } catch (SQLException e) {
            throw propagate(e);
        }
        return result;
    }

    @Override
    public void release() {

        try {
            Statement statement = resultSet.getStatement();
            statement.close();
            resultSet.close();
        } catch (SQLException e) {
            rethrow(e);
        }

    }
}
