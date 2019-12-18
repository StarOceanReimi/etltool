package com.limin.etltool.database;

import com.google.common.reflect.TypeResolver;
import com.limin.etltool.core.Batch;
import com.limin.etltool.core.EtlException;
import com.limin.etltool.database.util.DatabaseUtils;
import com.limin.etltool.database.util.JdbcSqlParamObject;
import com.limin.etltool.util.Exceptions;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Objects;

import static com.limin.etltool.util.Exceptions.propagate;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/18
 */
@Slf4j
public abstract class AbstractDbInput<T> extends DbSupport<T> implements DbInput<T> {

    public AbstractDbInput(Class<T> componentType, Database database, DatabaseAccessor accessor) {
        super(componentType, database, accessor);
        if(!accessor.accept(this))
            throw Exceptions.inform("Database Accessor does not support this source");
    }

    @Override
    public Batch<T> readInBatch(int batchSize) throws EtlException {

        JdbcSqlParamObject sqlParamObject = DatabaseUtils.buildSqlParamObject(accessor.getSql());
        try {
            String sql = sqlParamObject.getJdbcSql();
            PreparedStatement statement = connection.prepareStatement(sql);
            log.debug("SQL: {}", sql);
            if(!MapUtils.isEmpty(accessor.getParams())) {
                Object[] params = sqlParamObject.buildParam(accessor.getParams());
                DatabaseUtils.setParameters(statement, params);
                log.debug("Params: {}", Arrays.toString(params));
            }
            ResultSet resultSet = statement.executeQuery();
            return new DatabaseBatchObject<>(resultSet, batchSize, componentType);
        } catch (SQLException e) {
            throw propagate(e);
        }
    }
}





















