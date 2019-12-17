package com.limin.etltool.database;

import com.limin.etltool.core.EtlException;
import com.limin.etltool.core.OutputReport;
import com.limin.etltool.core.Source;
import com.limin.etltool.util.Exceptions;
import lombok.Data;
import org.apache.commons.collections.CollectionUtils;

import javax.sql.DataSource;
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

    private String table;

    private List<String> columns;

    private int batchSize;

    private OutputReport report;

    private final DatabaseOutputType databaseOutputType;

    private Class<T> componentType;

    public AbstractDatabaseOutput(DatabaseOutputType databaseOutputType, Class<T> componentType) {
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

        T sample = dataCollection.stream().findFirst().get();

        DatabaseSource databaseSource = (DatabaseSource) outputSource;
        optimizeForWrite(databaseSource);
        Connection connection = databaseSource.getConnection();
        PreparedStatement preparedStatement = buildOutputStatement(connection, sample);

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
            setStatementParameter(preparedStatement, data);
            try {
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

    private void setStatementParameter(PreparedStatement preparedStatement, T data) {
        Object[] args = buildParameters(data);

    }

    protected void optimizeForWrite(DatabaseSource databaseSource) {

    }

    protected Object[] buildParameters(T data) {

        return null;
    }

    protected PreparedStatement buildOutputStatement(Connection connection, T sample) {

        return null;
    }

}
