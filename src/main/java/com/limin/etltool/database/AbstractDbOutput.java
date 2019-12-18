package com.limin.etltool.database;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.limin.etltool.core.EtlException;
import com.limin.etltool.core.OutputReport;
import com.limin.etltool.database.util.DatabaseUtils;
import com.limin.etltool.database.util.IdKey;
import com.limin.etltool.database.util.JdbcSqlParamObject;
import com.limin.etltool.util.Exceptions;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.collections.CollectionUtils;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.limin.etltool.util.Exceptions.rethrow;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/18
 */
@Data
@Slf4j
public abstract class AbstractDbOutput<T> extends DbSupport<T> implements DbOutput<T> {

    private static final int DEFAULT_BATCH_SIZE = 1024;

    private int batchSize = DEFAULT_BATCH_SIZE;

    private OutputReport report = new DefaultDatabaseOutputReport();

    private int returnGeneratedKey = Statement.NO_GENERATED_KEYS;

    private String returnKeyName = null;

    private Object idKeyMethodOrField = null;

    public AbstractDbOutput(Class<T> componentType, Database database, DatabaseAccessor accessor) {
        super(componentType, database, accessor);
        if(!accessor.accept(this))
            throw Exceptions.inform("Database Accessor does not support this source");
    }

    @Override
    protected void initializeConnection(Database database) {
        database.getConfiguration().attribute("rewriteBatchedStatements", true);
        super.initializeConnection(database);
    }

    @Override
    public boolean writeCollection(Collection<T> dataCollection) throws EtlException {
        if(CollectionUtils.isEmpty(dataCollection)) return false;
        JdbcSqlParamObject paramObject = DatabaseUtils.buildSqlParamObject(accessor.getSql());
        try {
            connection.setAutoCommit(false);
            T sample = dataCollection.stream().findAny().get();
            PreparedStatement statement = buildPreparedStatement(paramObject, sample);
            int count = 0;
            List<T> temp = returnGeneratedKey == Statement.RETURN_GENERATED_KEYS ? Lists.newLinkedList() : null;
            for(T data : dataCollection) {
                if(count != 0 && count % batchSize == 0) {
                    executeBatchStatement(statement, temp);
                    if(temp != null) temp = Lists.newLinkedList();
                }
                DatabaseUtils.setParameters(statement, paramObject.buildParam(data));
                statement.addBatch();
                if(temp != null) temp.add(data);
                count++;
            }
            executeBatchStatement(statement, temp);
            connection.commit();
        } catch (SQLException e) {
            report.logErrorResult(e);
            try {
                connection.rollback();
            } catch (SQLException ex) {
                rethrow(ex);
            }
        }
        return !report.hasError();
    }

    private PreparedStatement buildPreparedStatement(JdbcSqlParamObject paramObject, T sample) throws SQLException {

        String sql = paramObject.getJdbcSql();
        if(sql.startsWith("INSERT")) {
            if(!Strings.isNullOrEmpty(accessor.getInsertedReturnKeyName())) {
                returnKeyName = accessor.getInsertedReturnKeyName();
                returnGeneratedKey = Statement.RETURN_GENERATED_KEYS;
            } else if(idKeyAnnoPresent(sample)) {
                returnGeneratedKey = Statement.RETURN_GENERATED_KEYS;
            }
        }
        return connection.prepareStatement(sql, returnGeneratedKey);
    }

    private boolean idKeyAnnoPresent(T sample) {
        if(sample instanceof Map) return false;
        val descs = PropertyUtils.getPropertyDescriptors(sample.getClass());
        for (val desc : descs) {
            Method read = desc.getReadMethod();
            if(read.isAnnotationPresent(IdKey.class)) {
                idKeyMethodOrField = desc.getWriteMethod();
                return true;
            }
        }
        val fields = sample.getClass().getDeclaredFields();
        for(val f : fields) {
            if(f.isAnnotationPresent(IdKey.class)) {
                idKeyMethodOrField = f;
                return true;
            }
        }
        return false;
    }

    private void executeBatchStatement(PreparedStatement statement, List<T> temp) throws SQLException {

        int[] result = statement.executeBatch();
        report.logSuccessResult(result);
        //需要回写数据库生成的主键
        if(temp != null) {
            ResultSet keys = statement.getGeneratedKeys();
            Iterator<T> iter = temp.iterator();
            while (iter.hasNext() && keys.next()) {
                Object key = keys.getObject(1);
                Object bean = iter.next();
                if((bean instanceof Map) && returnKeyName != null) {
                    ((Map) bean).put(returnKeyName, key);
                } else if(idKeyMethodOrField != null) {
                    if(idKeyMethodOrField instanceof Method) {
                        try {
                            ((Method) idKeyMethodOrField).invoke(bean, key);
                        } catch (IllegalAccessException | InvocationTargetException e) {
                            log.warn("cannot invoke setter: {}", idKeyMethodOrField);
                        }
                    } else if(idKeyMethodOrField instanceof Field) {
                        ((Field) idKeyMethodOrField).setAccessible(true);
                        try {
                            ((Field) idKeyMethodOrField).set(bean, key);
                        } catch (IllegalAccessException e) {
                            log.warn("cannot set field {}", idKeyMethodOrField);
                        }
                    }
                }
            }
        }
    }

}















