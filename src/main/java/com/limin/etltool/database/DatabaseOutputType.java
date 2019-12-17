package com.limin.etltool.database;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.limin.etltool.database.util.nameconverter.INameConverter;
import com.limin.etltool.database.util.nameconverter.NameConverter;
import com.limin.etltool.util.TemplateUtils;
import lombok.AllArgsConstructor;
import lombok.Data;
import net.sf.jsqlparser.statement.delete.Delete;
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

    int INSERT = 1;
    int UPDATE = 2;
    int DELETE = 3;

    List<String> getColumns();

    String getIdName();

    int getType();

    @Data
    @AllArgsConstructor
    class Insert implements DatabaseOutputType {

        private List<String> columns;

        @Override
        public String getIdName() {
            return null;
        }

        @Override
        public int getType() {
            return INSERT;
        }
    }

    @Data
    @AllArgsConstructor
    class Update implements DatabaseOutputType {

        private String idName;

        private List<String> columns;

        @Override
        public int getType() {
            return UPDATE;
        }
    }

    @Data
    @AllArgsConstructor
    class Delete implements DatabaseOutputType {

        private String idName;

        @Override
        public List<String> getColumns() {
            return null;
        }

        @Override
        public int getType() {
            return DELETE;
        }
    }
}
