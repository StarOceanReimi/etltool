package com.limin.etltool.database;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.limin.etltool.core.EtlException;
import com.limin.etltool.database.mysql.ColumnDefinition;
import com.limin.etltool.database.mysql.ColumnDefinitionHelper;
import com.limin.etltool.database.mysql.DefaultMySqlDatabase;
import com.limin.etltool.database.util.IdKey;
import com.limin.etltool.util.Exceptions;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.val;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.StatementVisitorAdapter;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.update.Update;
import org.apache.commons.collections.CollectionUtils;
import org.apache.ibatis.annotations.Results;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.limin.etltool.database.mysql.ColumnDefinition.INT;
import static com.limin.etltool.database.mysql.ColumnDefinition.VARCHAR;
import static com.limin.etltool.util.Exceptions.rethrow;
import static com.limin.etltool.util.TemplateUtils.logFormat;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/18
 */
public class NormalDbOutput<T> extends AbstractDbOutput<T> {

    protected NormalDbOutput(Database database, DatabaseAccessor accessor) {
        super(null, database, accessor);
    }

    public NormalDbOutput(Class<T> componentType, Database database, DatabaseAccessor accessor) {
        super(componentType, database, accessor);
    }

    private boolean createTableIfNotExists = false;

    private boolean truncateTableBeforeInsert = false;

    public void setCreateTableIfNotExists(boolean createTableIfNotExists) {
        this.createTableIfNotExists = createTableIfNotExists;
    }

    public void setTruncateTableBeforeInsert(boolean truncateTableBeforeInsert) {
        this.truncateTableBeforeInsert = truncateTableBeforeInsert;
    }

    @Override
    public boolean writeCollection(Collection<T> dataCollection) throws EtlException {
        if(CollectionUtils.isEmpty(dataCollection)) return false;
        if(createTableIfNotExists) {
            T data = dataCollection.stream().findAny().get();
            tryCreateTable(data);
        } else if(truncateTableBeforeInsert) {
            T data = dataCollection.stream().findAny().get();
            tryTruncateTable(data);
        }
        return super.writeCollection(dataCollection);
    }

    private void tryTruncateTable(T data) {
        String tableName = findTableName(data);
        if(checkTableExists(tableName)) {
            truncateTable(tableName);
        } else {
            throw Exceptions.inform("cannot truncate table that not existing." +
                    " please turn on createTableIfNotExists when table is not existing.");
        }
    }

    private String findTableName(T data) {
        String tableName = null;
        if(accessor instanceof TableColumnAccessor) {
            tableName = ((TableColumnAccessor) accessor).getTable();
        } else {
            AtomicReference<String> tableNameRef = new AtomicReference<>();
            try {
                Statement statement = CCJSqlParserUtil.parse(accessor.getSql(data));
                statement.accept(new StatementVisitorAdapter(){
                    @Override
                    public void visit(Insert insert) {
                        tableNameRef.set(insert.getTable().getName());
                    }

                    @Override
                    public void visit(Delete delete) {
                        tableNameRef.set(delete.getTable().getName());
                    }

                    @Override
                    public void visit(Update update) {
                        if(!update.getTables().isEmpty()) {
                            tableNameRef.set(update.getTables().get(0).getName());
                        }
                    }
                });
            } catch (JSQLParserException e) {
                e.printStackTrace();
            }
            tableName = tableNameRef.get();
        }
        if(tableName == null && !(data instanceof Map)) {
            tableName = data.getClass().getSimpleName();
        }
        if(tableName == null)
            throw Exceptions.inform("can not determine table name.");
        return tableName;
    }

    @SuppressWarnings("unchecked")
    private void tryCreateTable(T data) {
        String tableName = findTableName(data);
        if(!checkTableExists(tableName)) {
            List<ColumnDefinition> defs = null;
            if(data instanceof Map) {
                defs = ColumnDefinitionHelper.fromMap((Map<String, Object>) data);
            } else {
                defs = ColumnDefinitionHelper.fromClass(data.getClass());
            }
            checkArgument(!CollectionUtils.isEmpty(defs), "column defs cannot be empty.");
            database.createTable(tableName, null, defs);
        }
    }

    private interface StatementHandler<T> {
        T doWithStatement(java.sql.Statement statement) throws SQLException;
    }

    private interface ResultSetStatementHandler extends StatementHandler<ResultSet> { }

    private interface IntegerStatementHandler extends StatementHandler<Integer> { }

    private <T> T executeStatement(StatementHandler<T> call, Consumer<SQLException> statementErrorHandler) {

        java.sql.Statement statement = null;
        try {
            statement = connection.createStatement();
        } catch (SQLException e) {
            rethrow(e);
        }
        T result = null;
        try {
            result = call.doWithStatement(statement);
        } catch (SQLException e) {
            statementErrorHandler.accept(e);
        } finally {
            try {
                statement.close();
            } catch (SQLException e) {
                rethrow(e);
            }
        }
        return result;
    }

    private boolean truncateTable(final String tableName) {
        AtomicBoolean flag = new AtomicBoolean();
        executeStatement((IntegerStatementHandler) statement -> {
            boolean result = statement.execute(logFormat("TRUNCATE TABLE {}", tableName));
            flag.set(result);
            return 0;
        }, (e) -> flag.set(false));
        return flag.get();
    }

    private boolean checkTableExists(final String tableName) {
        AtomicBoolean flag = new AtomicBoolean();
        executeStatement((ResultSetStatementHandler) statement -> {
            ResultSet set = statement.executeQuery(logFormat("SELECT 1 FROM {}", tableName));
            flag.set(true);
            return set;
        }, (e) -> flag.set(false));
        return flag.get();
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class TestBean {

        @IdKey
        private String id;

        private String name;

    }

    public static void main(String[] args) throws EtlException {

        DatabaseConfiguration configuration = new DatabaseConfiguration();
        DefaultMySqlDatabase database = new DefaultMySqlDatabase(configuration);



    }
}
