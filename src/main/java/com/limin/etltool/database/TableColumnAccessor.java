package com.limin.etltool.database;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.limin.etltool.core.Source;
import com.limin.etltool.database.mysql.ColumnDefinitionHelper;
import com.limin.etltool.database.util.IdKey;
import com.limin.etltool.database.util.SqlBuilder;
import com.limin.etltool.database.util.nameconverter.INameConverter;
import com.limin.etltool.util.Exceptions;
import com.limin.etltool.util.ReflectionUtils;
import com.limin.etltool.util.TemplateUtils;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.val;
import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.collections.CollectionUtils;

import java.beans.FeatureDescriptor;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Collections.singletonList;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/18
 */
@Data
public class TableColumnAccessor implements DatabaseAccessor {

    static private Joiner joiner = Joiner.on(", ").skipNulls();

    public enum SqlType {
        INSERT, UPSERT, UPDATE, DELETE, SELECT;
        public boolean accept(Source source) {
            if((source instanceof DbInput) && this == SELECT) return true;
            if(source instanceof DbOutput)
                return this == INSERT || this == UPDATE || this == DELETE || this == UPSERT;
            return false;
        }
    }

    private final String table;

    private final SqlType type;

    private String insertedReturnKeyName;

    public TableColumnAccessor(String table) {
        this(SqlType.SELECT, table);
    }

    public TableColumnAccessor(SqlType type, String table) {
        checkArgument(!Strings.isNullOrEmpty(table), "table can not be empty");
        this.table = table;
        this.type = type;
    }

    private boolean insertIgnore = false;

    private List<String> columns = Lists.newLinkedList();

    private List<String> conditions = Lists.newLinkedList();

    public TableColumnAccessor insertIgnore(boolean insertIgnore) {
        this.insertIgnore = insertIgnore;
        return this;
    }

    public TableColumnAccessor insertedReturnKeyName(String name) {
        this.insertedReturnKeyName = name;
        return this;
    }

    public TableColumnAccessor column(String... columnName) {
        columns.addAll(Arrays.asList(columnName));
        return this;
    }

    public TableColumnAccessor andConditions(String... columnName) {
        conditions.addAll(Arrays.asList(columnName));
        return this;
    }

    @Override
    public String getSql(Object bean) {
        switch (type) {
            case INSERT:
                if(bean != null && CollectionUtils.isEmpty(columns))
                    setColumnsWithBean(bean);
                return SqlBuilder.insertBuilder()
                        .table(table).columns(columns).ignoreDuplicates(insertIgnore).buildSqlTemplate();
            case UPDATE:
                if(bean != null && CollectionUtils.isEmpty(columns))
                    setColumnsWithBean(bean);
                if(bean != null && CollectionUtils.isEmpty(conditions)) {
                    setConditions(singletonList(ReflectionUtils.findPropertyNameWithAnnotation(bean, IdKey.class)));
                }
                return SqlBuilder.updateBuilder()
                        .table(table).columns(columns)
                        .cond(conditions).buildSqlTemplate();
            case UPSERT:
                if(bean != null && CollectionUtils.isEmpty(columns))
                    setColumnsWithBean(bean);
                return SqlBuilder.upsertBuilder().table(table).columns(columns).buildSqlTemplate();
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

    @SuppressWarnings("unchecked")
    private void setColumnsWithBean(Object bean) {
        if(bean instanceof Map) {
            setColumns(Lists.newArrayList(((Map) bean).keySet()));
        } else {
            INameConverter converter = INameConverter.getConverter(bean.getClass());
            List<String> names = Arrays.stream(PropertyUtils.getPropertyDescriptors(bean.getClass()))
                    .map(FeatureDescriptor::getDisplayName)
                    .filter(name -> !"class".equals(name))
                    .map(converter::rename)
                    .collect(Collectors.toList());
            setColumns(names);
        }
    }

    @Override
    public Map<String, Object> getParams() {
        return null;
    }

    @Override
    public boolean accept(Source source) {
        return this.type.accept(source);
    }


    @Data
    @AllArgsConstructor
    public static class TestBeanClass {

        private String id;

        private String content;

    }

    public static void main(String[] args) {

        TestBeanClass testBeanClass = new TestBeanClass("test", "test");
        val defs = ColumnDefinitionHelper.fromClass(testBeanClass.getClass());
        String columnDefs = defs.stream().map(Object::toString).collect(Collectors.joining(","));
        String ddl = TemplateUtils.logFormat("CREATE TABLE {} ({})", "t_test", columnDefs);
        System.out.println(ddl);
    }
}
