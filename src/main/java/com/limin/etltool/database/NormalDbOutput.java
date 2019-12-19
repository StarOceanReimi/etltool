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

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkArgument;
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

    public void setCreateTableIfNotExists(boolean createTableIfNotExists) {
        this.createTableIfNotExists = createTableIfNotExists;
    }

    @Override
    public boolean writeCollection(Collection<T> dataCollection) throws EtlException {
        if(CollectionUtils.isEmpty(dataCollection)) return false;
        if(createTableIfNotExists) {
            T data = dataCollection.stream().findAny().get();
            tryCreateTable(data);
        }
        return super.writeCollection(dataCollection);
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

    private boolean checkTableExists(String tableName) {
        java.sql.Statement statement = null;
        try {
            statement = connection.createStatement();
        } catch (SQLException e) {
            rethrow(e);
        }
        try {
            statement.executeQuery(logFormat("SELECT 1 FROM {}", tableName));
        } catch (SQLException e) {
            return false;
        } finally {
            try {
                statement.close();
            } catch (SQLException e) {
                rethrow(e);
            }
        }
        return true;
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
        database.createTable("my_test", null, Arrays.asList(
                ColumnDefinition.builder().name("id").type(INT(10)).primaryKey(true).autoIncrement(true).build(),
                ColumnDefinition.builder().name("name").type(VARCHAR(255)).build()));

        DatabaseAccessor accessor = new TableColumnAccessor(TableColumnAccessor.SqlType.INSERT, "my_test")
                .column("name");
        NormalDbOutput<TestBean> output = new NormalDbOutput<TestBean>(database, accessor) {};
        List<TestBean> mapList = Lists.newArrayList();
        for(int i=0; i<100; i++) {
            mapList.add(new TestBean(null, "QL_" + String.format("%02d", i)));
        }
        val sw = Stopwatch.createStarted();
        output.writeCollection(mapList);
        System.out.println(mapList.get(5).getId());
        System.out.println(sw.stop());

    }
}
