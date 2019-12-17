package com.limin.etltool.database;

import com.google.common.base.Joiner;
import com.limin.etltool.core.Batch;
import com.limin.etltool.core.EtlException;
import com.limin.etltool.core.Source;
import com.limin.etltool.util.Exceptions;
import lombok.Data;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static com.limin.etltool.util.Exceptions.propagate;
import static com.limin.etltool.util.TemplateUtils.logFormat;
import static java.util.Optional.ofNullable;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/17
 */
@Data
public abstract class AbstractDatabaseInput<T> implements DatabaseInput<T> {

    private final String table;

    private List<String> columns;

    protected Class<T> componentType;

    private static final String SIMPLE_QUERY_TEMPLATE = "SELECT {} FROM {}";

    protected static final Joiner COMMA_JOINER = Joiner.on(",").skipNulls();

    public AbstractDatabaseInput(String table, Class<T> componentType) {
        this.table = table;
        if(componentType == null && getClass().getGenericSuperclass() instanceof ParameterizedType) {
            Type type = ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
            if(type instanceof Class)
                this.componentType = (Class<T>) type;
        } else {
            this.componentType = componentType;
        }
    }

    /**
     * 一般根据表明和字段名的查询实现
     * @return 查询语句
     */
    protected PreparedStatement buildPreparedStatement(Connection connection) {
        Objects.requireNonNull(connection);
        String columnsList = ofNullable(getColumns()).map(COMMA_JOINER::join).orElse("*");
        String t = getTable();
        checkArgument(t != null, "table can not be null");
        String sql = logFormat(SIMPLE_QUERY_TEMPLATE, columnsList, t);
        try {
            return connection.prepareStatement(sql);
        } catch (SQLException e) {
            throw propagate(e);
        }
    }

    @Override
    public Batch<T> readInBatch(Source inputSource, int batchSize) throws EtlException {

        if(!(inputSource instanceof DatabaseSource))
            throw Exceptions.inform("Database Input can only take database input source. {}", inputSource.getClass());

        DatabaseSource source = (DatabaseSource) inputSource;

        PreparedStatement statement = buildPreparedStatement(source.getConnection());

        try {
            ResultSet resultSet = statement.executeQuery();
            return new DatabaseBatchObject<>(resultSet, batchSize, componentType);
        } catch (SQLException e) {
            throw propagate(e);
        }
    }
}


















