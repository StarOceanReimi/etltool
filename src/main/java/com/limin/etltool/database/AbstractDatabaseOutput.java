package com.limin.etltool.database;

import com.limin.etltool.core.EtlException;
import com.limin.etltool.core.OutputReport;
import com.limin.etltool.core.Source;
import com.limin.etltool.database.util.JdbcSqlParamObject;
import com.limin.etltool.database.util.SqlBuilder;
import com.limin.etltool.util.Exceptions;
import lombok.Data;
import org.apache.commons.collections.CollectionUtils;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;

import static com.limin.etltool.util.Exceptions.rethrow;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/17
 */
@Data
public class AbstractDatabaseOutput<T> implements DatabaseOutput<T> {

    private List<String> columns;

    private int batchSize;

    private OutputReport report;

    private final DatabaseOutputType databaseOutputType;

    private Class<T> componentType;

    public AbstractDatabaseOutput(DatabaseOutputType databaseOutputType, Class<T> componentType) {
        this.report = new DatabaseOutputReport();
        this.databaseOutputType = databaseOutputType;
        if(componentType == null && getClass().getGenericSuperclass() instanceof ParameterizedType) {
            Type type = ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
            if(type instanceof Class)
                this.componentType = (Class<T>) type;
        } else {
            this.componentType = componentType;
        }
    }

    @Override
    public boolean writeCollection(Collection<T> dataCollection, Source outputSource) throws EtlException {

        if(!(outputSource instanceof DatabaseSource))
            throw Exceptions.inform("Database Input can only take database input source. {}", outputSource.getClass());

        if(CollectionUtils.isEmpty(dataCollection)) {
            report.logSuccessResult(new int[0]);
            return true;
        }

        DatabaseSource databaseSource = (DatabaseSource) outputSource;
        optimizeForWrite(databaseSource);
        Connection connection = databaseSource.getConnection();

        JdbcSqlParamObject jdbcSqlParamObject = buildJdbcSqlParamObject(getDatabaseOutputType());
        PreparedStatement preparedStatement = null;

        try {
            preparedStatement = buildOutputStatement(connection, jdbcSqlParamObject.getJdbcSql());
        } catch (SQLException e) {
            rethrow(e);
        }

        int count = 0;

        for(T data : dataCollection) {

            if(count != 0 && count % batchSize == 0) {
                try {
                    int[] affectedRows = preparedStatement.executeBatch();
                    report.logSuccessResult(affectedRows);
                } catch (SQLException e) {
                    report.logErrorResult(e);
                }
            }
            try {
                setStatementParameter(preparedStatement, data, jdbcSqlParamObject);
                preparedStatement.addBatch();
            } catch (SQLException e) {
                report.logErrorResult(e);
            }
            count++;
        }

        try {
            int[] affectedRows = preparedStatement.executeBatch();
            report.logSuccessResult(affectedRows);
        } catch (SQLException e) {
            report.logErrorResult(e);
        }

        try {
            connection.close();
            preparedStatement.close();
        } catch (SQLException e) {
            rethrow(e);
        }

        return report.hasError();
    }

//    private List<String> mergeColumns(T sample) {
//        Set<String> sampleColumns;
//        if(sample instanceof Map)
//            sampleColumns = ((Map) sample).keySet();
//        else {
//            PropertyDescriptor[] descriptors = PropertyUtils.getPropertyDescriptors(sample.getClass());
//            sampleColumns = Arrays.stream(descriptors).map(FeatureDescriptor::getDisplayName)
//                    .filter(name -> !name.equals("class"))
//                    .collect(Collectors.toSet());
//        }
//        if (CollectionUtils.isEmpty(columns)) {
//            columns = Lists.newArrayList();
//            columns.addAll(sa)
//        }
//
//    }

    private JdbcSqlParamObject buildJdbcSqlParamObject(DatabaseOutputType databaseOutputType) {


        return null;

    }

    private void setStatementParameter(
            PreparedStatement preparedStatement,
            T data, JdbcSqlParamObject jdbcSqlParamObject) throws SQLException {

        Object[] params = jdbcSqlParamObject.buildParam(data);
        for(int i = 1; i <= params.length; i++)
            preparedStatement.setObject(i, params[i-1]);
    }

    protected void optimizeForWrite(DatabaseSource databaseSource) {

    }

    protected PreparedStatement buildOutputStatement(Connection connection, String sql) throws SQLException {
        return connection.prepareStatement(sql);
    }

}
