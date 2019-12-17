package com.limin.etltool.database.util;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.limin.etltool.util.TemplateUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.limin.etltool.util.TemplateUtils.logFormat;
import static java.util.stream.Collectors.toMap;

@Slf4j
public abstract class SqlBuilder {

    static final Joiner COMMA_JOINER = Joiner.on(",").skipNulls();

    static final Joiner.MapJoiner SET_JOINER = Joiner.on(",").withKeyValueSeparator(" = ");

    protected String tableName;


    private SqlBuilder() {
    }

    private static final Pattern PARAMS_PATTERN = Pattern.compile("(:\\w+)");

    public JdbcSqlParamObject build() {

        String template = buildSqlTemplate();
        Matcher matcher = PARAMS_PATTERN.matcher(template);
        List<String> paramNames = Lists.newArrayList();
        StringBuffer result = new StringBuffer();
        while (matcher.find()) {
            String paramName = matcher.group(1).substring(1);
            paramNames.add(paramName);
            matcher.appendReplacement(result, "?");
        }
        matcher.appendTail(result);
        return new JdbcSqlParamObject(result.toString(), paramNames.toArray(new String[0]));
    }

    protected abstract String buildSqlTemplate();

    public static InsertBuilder insertBuilder() {

        return new InsertBuilder();
    }

    public static UpdateBuilder updateBuilder() {

        return new UpdateBuilder();
    }

    public static DeleteBuilder deleteBuilder() {

        return new DeleteBuilder();
    }

    public static class InsertBuilder extends SqlBuilder {

        private static final String INSERT_TPL = "INSERT INTO {} {} VALUES {}";

        @Override
        public String buildSqlTemplate() {
            checkArgument(!Strings.isNullOrEmpty(tableName), "tableName cannot be empty");
            checkArgument(!CollectionUtils.isEmpty(columnNames), "columnNames cannot be empty");
            String columns = "(" + COMMA_JOINER.join(columnNames) + ")";
            String placeHolder = "(" + columnNames.stream().map(n -> ":" + n).collect(Collectors.joining(",")) + ")";
            return logFormat(INSERT_TPL, tableName, columns, placeHolder);
        }

        private List<String> columnNames = Lists.newArrayList();

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
            checkArgument(!Strings.isNullOrEmpty(idName), "idName cannot be empty");

            Map<String, String> updates = updateNames.stream().collect(toMap(n -> n, n -> ":" + n));
            String updateFields = SET_JOINER.join(updates);
            String cond = logFormat("{} = {}", idName, ":" + idName);

            return TemplateUtils.logFormat(UPDATE_TPL, tableName, updateFields, cond);
        }

        private List<String> updateNames = Lists.newArrayList();

        private String idName;

        public UpdateBuilder table(String name) {
            this.tableName = name;
            return this;
        }

        public UpdateBuilder column(String name) {
            updateNames.add(name);
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

        public UpdateBuilder idName(String idName) {
            this.idName = idName;
            return this;
        }

    }

    public static class DeleteBuilder extends SqlBuilder {

        private static final String DELETE_TPL = "DELETE FROM {} WHERE {}";

        private String idName;

        @Override
        public String buildSqlTemplate() {
            checkArgument(!Strings.isNullOrEmpty(tableName), "tableName cannot be empty");
            checkArgument(!Strings.isNullOrEmpty(idName), "idName cannot be empty");
            return logFormat(DELETE_TPL, tableName, logFormat("{} = {}", idName, ":" + idName));
        }

        public DeleteBuilder table(String name) {
            this.tableName = name;
            return this;
        }

        public DeleteBuilder idName(String idName) {
            this.idName = idName;
            return this;
        }

    }

    public static void main(String[] args) {

        Map<String, Object> map = Maps.newHashMap();
        map.put("id", 123L);
        map.put("name", "QL");
        map.put("age", 33);

        JdbcSqlParamObject paramObject =
                SqlBuilder.insertBuilder().columns("id", "name", "age", "sex").table("my_test").build();

        System.out.println(paramObject.getJdbcSql());
        System.out.println(Arrays.toString(paramObject.buildParam(map)));

    }

}
