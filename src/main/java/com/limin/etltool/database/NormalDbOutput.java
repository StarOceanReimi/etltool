package com.limin.etltool.database;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.limin.etltool.core.EtlException;
import com.limin.etltool.core.OutputReport;
import com.limin.etltool.database.mysql.ColumnDefinition;
import com.limin.etltool.database.mysql.ColumnDefinitionHelper;
import com.limin.etltool.database.mysql.DefaultMySqlDatabase;
import com.limin.etltool.database.util.IdKey;
import com.limin.etltool.util.Exceptions;
import com.limin.etltool.work.Flow;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.StatementVisitorAdapter;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.update.Update;
import org.apache.commons.collections.CollectionUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.limin.etltool.util.Exceptions.rethrow;
import static com.limin.etltool.util.TemplateUtils.logFormat;
import static java.util.Comparator.comparing;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/18
 */
@Slf4j
public class NormalDbOutput<T> extends AbstractDbOutput<T> {

    public static final ThreadLocal<Boolean> globalMultiThreadEnvironment = ThreadLocal.withInitial(() -> false);

    protected NormalDbOutput(Database database, DatabaseAccessor accessor) {
        super(null, database, accessor);
    }

    public NormalDbOutput(Class<T> componentType, Database database, DatabaseAccessor accessor) {
        super(componentType, database, accessor);
    }

    private boolean createTableIfNotExists = false;

    private boolean truncateTableBeforeInsert = false;

    private boolean onlyTruncateInFirstTimeInBatch = true;

    private boolean useDeleteFrom = false;

    private boolean multiThreadMode = false;

    private String[] primaryKeyNames = null;

    private ColumnDefinition.Index[] indexList;

    public void setTableIndicesIfAutoCreateTable(ColumnDefinition.Index... indices) {
        this.indexList = indices;
    }

    public void setPrimaryKeyNameIfAutoCreateTable(String... primaryKeyName) {
        this.primaryKeyNames = primaryKeyName;
    }

    public void setMultiThreadMode(boolean multiThreadMode) {
        this.multiThreadMode = multiThreadMode;
    }

    public void setCreateTableIfNotExists(boolean createTableIfNotExists) {
        this.createTableIfNotExists = createTableIfNotExists;
    }

    public void setTruncateTableBeforeInsert(boolean truncateTableBeforeInsert) {
        this.truncateTableBeforeInsert = truncateTableBeforeInsert;
    }

    public void setOnlyTruncateInFirstTimeInBatch(boolean truncateAtFirstTime) {
        this.onlyTruncateInFirstTimeInBatch = truncateAtFirstTime;
    }

    public void setUseDeleteFrom(boolean useDeleteFrom) {
        this.useDeleteFrom = useDeleteFrom;
    }

    private final Map<String, ColumnDefinition.ColumnType> typeConfig = Maps.newHashMap();

    public void registerType(String name, Class<?> type) {
        Preconditions.checkState(!Strings.isNullOrEmpty(name), "type name cannot be empty");
        Preconditions.checkNotNull(type, "type cannot be null");
        typeConfig.put(name, ColumnDefinition.ColumnType.guessFromType(type));
    }

    public void registerType(String name, ColumnDefinition.ColumnType type) {
        Preconditions.checkState(!Strings.isNullOrEmpty(name), "type name cannot be empty");
        Preconditions.checkNotNull(type, "type cannot be null");
        typeConfig.put(name, type);
    }

    @Override
    public boolean writeCollection(Collection<T> dataCollection) throws EtlException {
        if (CollectionUtils.isEmpty(dataCollection)) return false;
        if (createTableIfNotExists) {
            T data = dataCollection.stream().findAny().get();
            tryCreateTable(data);
        }

        if (truncateTableBeforeInsert) {
            T data = dataCollection.stream().findAny().get();
            tryTruncateTable(data);
            if (onlyTruncateInFirstTimeInBatch)
                truncateTableBeforeInsert = false;
        }

        if (isMultiThreadMode())
            return writeWithMultiThread(dataCollection);

        return super.writeCollection(dataCollection);

    }

    public boolean isMultiThreadMode() {
        return multiThreadMode || globalMultiThreadEnvironment.get();
    }

    private boolean writeWithMultiThread(Collection<T> dataCollection) throws EtlException {

        final int processors = Runtime.getRuntime().availableProcessors() * 2;
        int size = dataCollection.size() / (processors * 2) + 1;
        final List<Throwable> exceptionList = Lists.newCopyOnWriteArrayList();
        ThreadPoolExecutor executorService = createMultiThreadExecutor(exceptionList, processors);
        final Iterable<List<T>> subJobs = Iterables.partition(dataCollection, size);
        final CountDownLatch latch = new CountDownLatch(Iterables.size(subJobs));
        for (List<T> job : subJobs) {

            executorService.execute(() -> {
                try {
                    super.writeCollection(job);
                } catch (EtlException e) {
                    Exceptions.rethrow(e);
                } finally {
                    latch.countDown();
                }
            });
        }

        try {
            latch.await();
            return true;
        } catch (InterruptedException ie) {
            //ignore from interrupt
            return false;
        } finally {
            shutdownExecutor(executorService, exceptionList);
        }

    }

    private void shutdownExecutor(ThreadPoolExecutor executorService, List<Throwable> exceptionList) throws EtlException {
        try {
            executorService.shutdown();
            executorService.awaitTermination(5, TimeUnit.SECONDS);
            if (!exceptionList.isEmpty())
                throw processingExceptionList(exceptionList);
        } catch (InterruptedException ex) {
            //ignore
            executorService.shutdownNow();
        }
    }

    private ThreadPoolExecutor createMultiThreadExecutor(List<Throwable> exceptionList, int processors) {
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(processors);
        AtomicInteger counter = new AtomicInteger(0);
        Thread.UncaughtExceptionHandler handler = (t, err) -> exceptionList.add(err);
        executor.setThreadFactory(r -> {
            Thread t = new Thread(r, "EtlOutput-" + counter.getAndDecrement());
            t.setDaemon(true);
            t.setUncaughtExceptionHandler(handler);
            return t;
        });
        return executor;
    }

    private EtlException processingExceptionList(List<Throwable> exceptionList) {
        final List<String> messages = Lists.newArrayList();
        for (Throwable err : exceptionList) {
            if (err.getCause() != null && err.getCause() instanceof EtlException) {
                messages.add(err.getCause().getMessage());
            } else {
                messages.add(err.getMessage());
            }
        }
        return new EtlException(String.join("\n", messages));
    }

    private void tryTruncateTable(T data) {
        String tableName = findTableName(data);
        if (checkTableExists(tableName)) {
            truncateTable(tableName);
        } else {
            throw Exceptions.inform("cannot truncate table that not existing." +
                    " please turn on createTableIfNotExists when table is not existing.");
        }
    }

    private String findTableName(T data) {
        String tableName = null;
        if (accessor instanceof TableColumnAccessor) {
            tableName = ((TableColumnAccessor) accessor).getTable();
        } else {
            AtomicReference<String> tableNameRef = new AtomicReference<>();
            try {
                Statement statement = CCJSqlParserUtil.parse(accessor.getSql(data));
                statement.accept(new StatementVisitorAdapter() {
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
                        if (!update.getTables().isEmpty()) {
                            tableNameRef.set(update.getTables().get(0).getName());
                        }
                    }
                });
            } catch (JSQLParserException e) {
                e.printStackTrace();
            }
            tableName = tableNameRef.get();
        }
        if (tableName == null && !(data instanceof Map)) {
            tableName = data.getClass().getSimpleName();
        }
        if (tableName == null)
            throw Exceptions.inform("can not determine table name.");
        return tableName;
    }

    @SuppressWarnings("unchecked")
    private void tryCreateTable(T data) {
        String tableName = findTableName(data);
        if (!checkTableExists(tableName)) {
            List<ColumnDefinition> defs = null;
            if (data instanceof Map) {
                defs = ColumnDefinitionHelper.fromMap((Map<String, Object>) data, typeConfig);
            } else {
                defs = ColumnDefinitionHelper.fromClass(data.getClass(), typeConfig);
            }
            checkArgument(!CollectionUtils.isEmpty(defs), "column defs cannot be empty.");
            if (accessor instanceof TableColumnAccessor) {
                List<String> columns = ((TableColumnAccessor) accessor).getColumns();
                if (CollectionUtils.isNotEmpty(columns)) {
                    defs = defs.stream()
                            .filter(d -> columns.contains(d.getName()))
                            .collect(Collectors.toList());
                }
            }
            defs.sort(comparing(ColumnDefinition::getName));
            database.createTable(tableName, null, defs, primaryKeyNames, indexList);
        }
    }

    private interface StatementHandler<T> {
        T doWithStatement(java.sql.Statement statement) throws SQLException;
    }

    private interface ResultSetStatementHandler extends StatementHandler<ResultSet> {
    }

    private interface IntegerStatementHandler extends StatementHandler<Integer> {
    }

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
            String truncateSql = useDeleteFrom ?
                    logFormat("DELETE FROM {}", tableName) : logFormat("TRUNCATE TABLE {}", tableName);
            boolean result = statement.execute(truncateSql);
            flag.set(result);
            return 0;
        }, (e) -> flag.set(false));
        return flag.get();
    }

    public boolean checkTableExists(final String tableName) {
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
        DatabaseConfiguration conf1 = new DatabaseConfiguration("classpath:database.yml");
        DatabaseConfiguration conf2 = new DatabaseConfiguration("classpath:database1.yml");
        Database source = new DefaultMySqlDatabase(conf2);
        DefaultMySqlDatabase target = new DefaultMySqlDatabase(conf1);
        DatabaseAccessor sourceAccessor = new DefaultDatabaseAccessor("select id, ifnull(name, '') name from dangjian.fact_user");
        TableColumnAccessor targetAccessor = new TableColumnAccessor(TableColumnAccessor.SqlType.INSERT, "test.fact_user");
        NormalDbInput<Map<String, Object>> input = new NormalDbInput<Map<String, Object>>(source, sourceAccessor) {};
        NormalDbOutput<Map<String, Object>> output = new NormalDbOutput<Map<String, Object>>(target, targetAccessor) {};
        output.setCreateTableIfNotExists(true);
        output.setTruncateTableBeforeInsert(true);
        output.setIsolationLevel(Connection.TRANSACTION_READ_UNCOMMITTED);
        output.setMultiThreadMode(true);
        Flow flow = new Flow();
        flow.processInBatch(Integer.MAX_VALUE, input, o -> o, output);
        log.info("done");
    }
}
