package com.limin.etltool.database.util;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.limin.etltool.util.TemplateUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;

import java.util.*;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.limin.etltool.util.TemplateUtils.logFormat;
import static java.util.stream.Collectors.toMap;

@Slf4j
public abstract class SqlBuilder {

    static final Joiner COMMA_JOINER = Joiner.on(",").skipNulls();

    static final Joiner.MapJoiner SET_JOINER = Joiner.on(",").withKeyValueSeparator(" = ");
    static final Joiner.MapJoiner AND_WHERE_JOINER = Joiner.on(" and ").withKeyValueSeparator(" = ");

    protected String tableName;

    private SqlBuilder() {
    }

    public static UpsertBuilder upsertBuilder() {
        return new UpsertBuilder();
    }


    public JdbcSqlParamObject build() {

        String template = buildSqlTemplate();
        return DatabaseUtils.buildSqlParamObject(template);
    }

    public abstract String buildSqlTemplate();

    public static InsertBuilder insertBuilder() {

        return new InsertBuilder();
    }

    public static UpdateBuilder updateBuilder() {

        return new UpdateBuilder();
    }

    public static DeleteBuilder deleteBuilder() {

        return new DeleteBuilder();
    }

    public static class UpsertBuilder extends SqlBuilder {

        static final String UPSERT_TPL = "INSERT INTO {} {} VALUES {} ON DUPLICATE KEY UPDATE {}";

        private List<String> columnNames = Lists.newArrayList();
        private String tableName;

        public UpsertBuilder table(String name) {
            this.tableName = name;
            return this;
        }

        public UpsertBuilder column(String name) {
            columnNames.add(name);
            return this;
        }

        public UpsertBuilder columns(String... names) {
            columnNames.addAll(Arrays.asList(names));
            return this;
        }

        public UpsertBuilder columns(List<String> names) {
            columnNames.addAll(names);
            return this;
        }

        @Override
        public String buildSqlTemplate() {
            checkArgument(!Strings.isNullOrEmpty(tableName), "tableName cannot be empty");
            checkArgument(!CollectionUtils.isEmpty(columnNames), "columnNames cannot be empty");
            String columns = "(" + COMMA_JOINER.join(columnNames) + ")";
            String placeHolder = "(" + columnNames.stream().map(n -> ":" + n).collect(Collectors.joining(",")) + ")";
            BinaryOperator<String> merge = (s1, s2) -> { throw new RuntimeException("conflicts found."); };
            LinkedHashMap<String, String> map = columnNames.stream()
                    .collect(toMap(n -> n, n -> "VALUES(" + n + ")", merge, LinkedHashMap::new));
            String updates = Joiner.on(", ").withKeyValueSeparator("=").join(map);
            return logFormat(UPSERT_TPL, tableName, columns, placeHolder, updates);
        }
    }

    public static class InsertBuilder extends SqlBuilder {

        private static final String INSERT_TPL = "INSERT {} INTO {} {} VALUES {}";

        @Override
        public String buildSqlTemplate() {
            checkArgument(!Strings.isNullOrEmpty(tableName), "tableName cannot be empty");
            checkArgument(!CollectionUtils.isEmpty(columnNames), "columnNames cannot be empty");
            String columns = "(" + COMMA_JOINER.join(columnNames) + ")";
            String placeHolder = "(" + columnNames.stream().map(n -> ":" + n).collect(Collectors.joining(",")) + ")";
            return logFormat(INSERT_TPL, ignoreDuplicates ? "IGNORE" : "", tableName, columns, placeHolder);
        }

        private List<String> columnNames = Lists.newArrayList();

        private boolean ignoreDuplicates = false;

        public InsertBuilder ignoreDuplicates(boolean ignoreDuplicates) {
            this.ignoreDuplicates = ignoreDuplicates;
            return this;
        }

        public InsertBuilder table(String name) {
            this.tableName = name;
            return this;
        }

        public InsertBuilder column(String name) {
            columnNames.add(name);
            return this;
        }

        public InsertBuilder columns(String... names) {
            columnNames.addAll(Arrays.asList(names));
            return this;
        }

        public InsertBuilder columns(List<String> names) {
            columnNames.addAll(names);
            return this;
        }

    }


    public static class UpdateBuilder extends SqlBuilder {

        private static final String UPDATE_TPL = "UPDATE {} SET {} WHERE {}";

        @Override
        public String buildSqlTemplate() {

            checkArgument(!Strings.isNullOrEmpty(tableName), "tableName cannot be empty");
            checkArgument(!CollectionUtils.isEmpty(updateNames), "updateNames cannot be empty");
            checkArgument(!CollectionUtils.isEmpty(conditionNames), "conditionNames cannot be empty");

            Map<String, String> updates = updateNames.stream().collect(toMap(n -> n, n -> ":" + n));
            String updateFields = SET_JOINER.join(updates);

            Map<String, String> conds = conditionNames.stream().collect(toMap(n -> n, n -> ":" + n));
            String condFields = AND_WHERE_JOINER.join(conds);

            return TemplateUtils.logFormat(UPDATE_TPL, tableName, updateFields, condFields);
        }

        private List<String> updateNames = Lists.newArrayList();

        private List<String> conditionNames = Lists.newArrayList();

        public UpdateBuilder table(String name) {
            this.tableName = name;
            return this;
        }

        public UpdateBuilder columns(String... names) {
            updateNames.addAll(Arrays.asList(names));
            return this;
        }

        public UpdateBuilder columns(List<String> names) {
            updateNames.addAll(names);
            return this;
        }

        public UpdateBuilder cond(String... names) {
            conditionNames.addAll(Arrays.asList(names));
            return this;
        }

        public UpdateBuilder cond(List<String> names) {
            conditionNames.addAll(names);
            return this;
        }

    }

    public static class DeleteBuilder extends SqlBuilder {

        private static final String DELETE_TPL = "DELETE FROM {} WHERE {}";

        private List<String> conditionNames = Lists.newArrayList();

        @Override
        public String buildSqlTemplate() {
            checkArgument(!Strings.isNullOrEmpty(tableName), "tableName cannot be empty");
            checkArgument(!CollectionUtils.isEmpty(conditionNames), "conditionNames cannot be empty");
            Map<String, String> conds = conditionNames.stream().collect(toMap(n -> n, n -> ":" + n));
            String condFields = AND_WHERE_JOINER.join(conds);
            return logFormat(DELETE_TPL, tableName, condFields);
        }

        public DeleteBuilder table(String name) {
            this.tableName = name;
            return this;
        }

        public DeleteBuilder cond(String... names) {
            conditionNames.addAll(Arrays.asList(names));
            return this;
        }

        public DeleteBuilder cond(List<String> names) {
            conditionNames.addAll(names);
            return this;
        }

    }

    public static void main(String[] args) {

        Map<String, Object> map = Maps.newHashMap();
        map.put("id", 123L);
        map.put("name", "QL");
        map.put("age", 33);

        JdbcSqlParamObject paramObject =
                SqlBuilder.upsertBuilder()
                        .table("my_test")
                        .columns("id", "name", "age")
                        .build();

        System.out.println(paramObject.getJdbcSql());
        System.out.println(Arrays.toString(paramObject.buildParam(map)));

    }

}
