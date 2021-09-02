package com.limin.etltool.database.mysql;

import com.limin.etltool.database.AbstractDatabase;
import com.limin.etltool.database.Database;
import com.limin.etltool.database.DatabaseConfiguration;
import com.limin.etltool.database.StatementCallback;
import com.limin.etltool.util.TemplateUtils;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.*;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static com.limin.etltool.database.mysql.ColumnDefinition.BIGINT;
import static com.limin.etltool.database.mysql.ColumnDefinition.VARCHAR;
import static com.limin.etltool.util.Exceptions.propagate;
import static com.limin.etltool.util.Exceptions.rethrow;
import static com.limin.etltool.util.QueryStringUtils.toQueryString;
import static com.limin.etltool.util.QueryStringUtils.wrapToQueryStringMap;
import static java.util.Optional.ofNullable;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/18
 */
@Slf4j
public class DefaultMySqlDatabase extends AbstractDatabase {

    public DefaultMySqlDatabase(DatabaseConfiguration configuration) {
        super(configuration);
    }

    @Override
    public boolean createTable(String table, String tableComment, List<ColumnDefinition> defs) {
        String columnDefs = defs.stream().map(Object::toString).collect(Collectors.joining(","));
        String comment = ofNullable(tableComment).map(c -> "COMMENT '" + c + "'").orElse("");
        String ddl = TemplateUtils.logFormat(COMMON_CREATE_TABLE_TPL, table, columnDefs, comment);
        return executeSQL(ddl);
    }

    private static final String COMMON_CREATE_TABLE_TPL = "CREATE TABLE {} ({}) {}";

    @Override
    public void optimizeForBatchWriting() {
        getConfiguration().attribute("rewriteBatchedStatements", true);
    }


    @Override
    public boolean createTable(String table,
                               String tableComment,
                               List<ColumnDefinition> defs,
                               String[] primaryKeys,
                               ColumnDefinition.Index[] indices) {
        if (indices == null && primaryKeys == null)
            return createTable(table, tableComment, defs);
        String columnDefs = defs.stream().peek(def -> def.setPrimaryKey(false)).map(Object::toString).collect(Collectors.joining(","));
        String comment = ofNullable(tableComment).map(c -> "COMMENT '" + c + "'").orElse("");
        String indexOption = ofNullable(indices).map(i -> Arrays.stream(i).map(idx -> "index " + idx.toString())
                .collect(Collectors.joining(","))).map(s -> "," + s).orElse("");
        String primaryOption = primaryKeys != null && primaryKeys.length > 0 ?
                " ,PRIMARY KEY (" + String.join(",", primaryKeys) + ")" : "";
        String body = columnDefs + primaryOption + indexOption;
        String ddl = TemplateUtils.logFormat(COMMON_CREATE_TABLE_TPL, table, body, comment);
        return executeSQL(ddl);
    }

}
