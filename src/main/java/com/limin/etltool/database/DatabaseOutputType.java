package com.limin.etltool.database;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.limin.etltool.database.util.nameconverter.INameConverter;
import com.limin.etltool.database.util.nameconverter.NameConverter;
import com.limin.etltool.util.TemplateUtils;
import lombok.Data;
import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.collections.CollectionUtils;

import java.beans.PropertyDescriptor;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/17
 */
public interface DatabaseOutputType {

    Joiner COMMA_JOINER = Joiner.on(",").skipNulls();

    String getSqlTemplate(String table, List<String> columns, Object sample);

    Object[] buildSqlParams(Object sample);

    String getIdField();

    class Insert implements DatabaseOutputType {

        private static final String INSERTION_TEMPLATE = "INSERT INTO {} VALUES {}";


        @Override
        public String getSqlTemplate(String table, List<String> columns, Object sample) {
            Set<String> sampleColumns;
            if(sample instanceof Map) {
                sampleColumns = ((Map) sample).keySet();
            } else {
                PropertyDescriptor[] descriptors = PropertyUtils.getPropertyDescriptors(sample);
                INameConverter converter = INameConverter.getConverter(sample.getClass());
                sampleColumns = Arrays.stream(descriptors)
                        .map(p -> converter.rename(p.getDisplayName())).collect(toSet());
            }

            if (CollectionUtils.isEmpty(columns)) {
                columns = Lists.newArrayList(sampleColumns);
            }
            else {
                columns = columns.stream().filter(sampleColumns::contains).collect(toList());
            }

            String columnString = COMMA_JOINER.join(columns);
            String placeHolder = IntStream.range(0, columns.size()).mapToObj(i -> "?").collect(Collectors.joining(","));

            return TemplateUtils.logFormat(INSERTION_TEMPLATE, columnString, "(" + placeHolder + ")");
        }

        @Override
        public Object[] buildSqlParams(Object sample) {
            return new Object[0];
        }

        @Override
        public String getIdField() { return null; }
    }

    class Update implements DatabaseOutputType {

        private String idField;

        @Override
        public String getSqlTemplate(String table, List<String> columns, Object sample) {
            return null;
        }

        @Override
        public Object[] buildSqlParams(Object sample) {
            return new Object[0];
        }

        @Override
        public String getIdField() {
            return idField;
        }
    }

    class Delete implements DatabaseOutputType {

        private String idField;

        @Override
        public String getSqlTemplate(String table, List<String> columns, Object sample) {
            return null;
        }

        @Override
        public Object[] buildSqlParams(Object sample) {
            return new Object[0];
        }

        @Override
        public String getIdField() {
            return idField;
        }
    }

}
