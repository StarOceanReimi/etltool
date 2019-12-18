package com.limin.etltool.database;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.limin.etltool.core.Source;
import com.limin.etltool.database.util.SqlBuilder;
import com.limin.etltool.util.Exceptions;
import com.limin.etltool.util.TemplateUtils;
import lombok.Data;
import org.apache.commons.collections.CollectionUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Optional.ofNullable;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/18
 */
@Data
public class TableColumnAccessor implements DatabaseAccessor {

    static private Joiner joiner = Joiner.on(", ").skipNulls();

    public enum SqlType {
        INSERT, UPDATE, DELETE, SELECT;
        public boolean accept(Source source) {
            if((source instanceof DbInput) && this == SELECT) return true;
            if(source instanceof DbOutput)
                return this == INSERT || this == UPDATE || this == DELETE;
            return false;
        }
    }

    private final String table;

    private final SqlType type;

    public TableColumnAccessor(SqlType type, String table) {
        checkArgument(!Strings.isNullOrEmpty(table), "table can not be empty");
        this.table = table;
        this.type = type;
    }

    private List<String> columns = Lists.newLinkedList();

    private List<String> conditions = Lists.newLinkedList();

    public TableColumnAccessor column(String... columnName) {
        columns.addAll(Arrays.asList(columnName));
        return this;
    }

    public TableColumnAccessor andConditions(String... columnName) {
        conditions.addAll(Arrays.asList(columnName));
        return this;
    }


    @Override
    public String getSql() {
        switch (type) {
            case INSERT:
                return SqlBuilder.insertBuilder()
                        .table(table).columns(columns).buildSqlTemplate();
            case UPDATE:
                return SqlBuilder.updateBuilder()
                        .table(table).columns(columns)
                        .cond(conditions).buildSqlTemplate();
            case DELETE:
                return SqlBuilder.deleteBuilder()
                        .table(table)
                        .cond(conditions).buildSqlTemplate();
            case SELECT:
                String template = "SELECT {} FROM {}";
                String cols = CollectionUtils.isEmpty(columns) ? "*" : joiner.join(columns);
                return TemplateUtils.logFormat(template, cols, table);
        }
        throw Exceptions.unsupported("not expected exception");
    }

    @Override
    public Map<String, Object> getParams() {
        return null;
    }

    @Override
    public boolean accept(Source source) {
        return this.type.accept(source);
    }

    public static void main(String[] args) {

    }
}
