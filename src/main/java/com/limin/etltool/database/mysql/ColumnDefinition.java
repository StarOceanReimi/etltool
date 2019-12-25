package com.limin.etltool.database.mysql;

import com.google.common.base.Strings;
import lombok.AllArgsConstructor;
import lombok.Builder;
import org.apache.commons.collections.CollectionUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.time.*;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.Optional.ofNullable;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/19
 */
@Builder
public class ColumnDefinition {

    private String name;

    private ColumnType type;

    private boolean nullable = false;

    private boolean unique = false;

    private boolean primaryKey = false;

    private boolean autoIncrement = false;

    private String comment;

    private String defaultValue;

    private Index index;

    public static Index HASH(String name, String... refs) {
        return new Index(name, "USING HASH", Arrays.asList(refs));
    }

    public static Index BTREE(String name, String... refs) {
        return new Index(name, "USING BTREE", Arrays.asList(refs));
    }

    public static ColumnType INT(Integer length) {
        return new ColumnType("INT", ofNullable(length).orElse(11), null);
    }

    public static ColumnType BIGINT(Integer length) {
        return new ColumnType("BIGINT", ofNullable(length).orElse(20), null);
    }

    public static ColumnType TINYINT(Integer length) {
        return new ColumnType("TINYINT", ofNullable(length).orElse(4), null);
    }

    public static ColumnType VARCHAR(Integer length) {
        return new ColumnType("VARCHAR", ofNullable(length).orElse(255), null);
    }

    public static ColumnType TEXT() {
        return new ColumnType("TEXT", null, null);
    }

    public static ColumnType LONGTEXT() {
        return new ColumnType("LONGTEXT", null, null);
    }

    public static ColumnType DECIMAL(Integer length, Integer precision) {
        return new ColumnType("DECIMAL", ofNullable(length).orElse(11), ofNullable(precision).orElse(4));
    }

    public static ColumnType DATE() {
        return new ColumnType("DATE", null, null);
    }

    public static ColumnType DATETIME() {
        return new ColumnType("DATETIME", null, null);
    }

    private static ColumnType TIME() {
        return new ColumnType("TIME", null, null);
    }

    private static ColumnType TIMESTAMP() {
        return new ColumnType("TIMESTAMP", null, null);
    }


    @AllArgsConstructor
    public static class Index {

        private String name;

        private String type;

        private List<String> refs;

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("`").append(name).append("` ");
            if(!Strings.isNullOrEmpty(type))
                builder.append(type).append(" ");
            String refStr = refs.stream().map(ref -> "`" + ref + "`").collect(Collectors.joining(","));
            return builder.append("(").append(refStr).append(")").toString();
        }
    }

    @AllArgsConstructor
    public static class ColumnType {

        public static ColumnType guessFromType(Class<?> clazz) {
            Objects.requireNonNull(clazz);
            if(clazz == Integer.class || clazz == Integer.TYPE)
                return INT(null);
            if(clazz == Long.class || clazz == Long.TYPE)
                return BIGINT(null);
            if(clazz == Character.class || clazz == Character.TYPE)
                return TINYINT(null);
            if(clazz == Byte.class || clazz == Byte.TYPE)
                return TINYINT(null);
            if(clazz == Short.class || clazz == Short.TYPE)
                return INT(null);
            if(clazz == Double.class || clazz == Double.TYPE)
                return DECIMAL(11, 4);
            if(clazz == Float.class || clazz == Float.TYPE)
                return DECIMAL(6, 2);
            if(clazz == BigDecimal.class)
                return DECIMAL(11, 4);
            if(clazz == BigInteger.class)
                return BIGINT(20);
            if(clazz == Boolean.class || clazz == Boolean.TYPE)
                return TINYINT(1);
            if(CharSequence.class.isAssignableFrom(clazz))
                return VARCHAR(null);
            if(clazz == Date.class ||
                    clazz == Timestamp.class ||
                    clazz == LocalDateTime.class ||
                    clazz == ZonedDateTime.class)
                return DATETIME();
            if(clazz == LocalDate.class)
                return DATE();
            if(clazz == LocalTime.class)
                return TIME();
            if(clazz == Instant.class)
                return TIMESTAMP();
            return null;
        }

        public static ColumnType guessFromName(String name) {

            String ignoreCase = name.toLowerCase();
            if(ignoreCase.contains("id")) return BIGINT(null);
            if(ignoreCase.contains("text")
                    || ignoreCase.contains("remark")
                    || ignoreCase.contains("content"))
                return TEXT();
            if(ignoreCase.contains("date")
                    || ignoreCase.contains("modify_at")
                    || ignoreCase.contains("time"))
                return DATETIME();

            return null;
        }

        private String typeName;

        private Integer length;

        private Integer precision;

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append(typeName);
            ofNullable(length).ifPresent(l -> {
                builder.append("(").append(l);
                ofNullable(precision).ifPresent(p -> builder.append(",").append(precision));
                builder.append(")");
            });
            return builder.toString();
        }
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(name).append(" ")
               .append(type).append(" ");
        if(!nullable) builder.append("NOT ");
        builder.append("NULL ");
        if(primaryKey) builder.append("PRIMARY KEY ");
        else if(unique) builder.append("UNIQUE KEY ");
        if(!Strings.isNullOrEmpty(defaultValue))
            builder.append("DEFAULT '").append(defaultValue).append("' ");
        if(!Strings.isNullOrEmpty(comment))
            builder.append("COMMENT '").append(comment).append("' ");
        if(autoIncrement)
            builder.append("AUTO_INCREMENT ");
        if(!primaryKey && !unique && Objects.nonNull(index)) {
            if(CollectionUtils.isEmpty(index.refs))
                index.refs = Collections.singletonList(name);
            builder.append(", ").append("INDEX ").append(index.toString());
        }
        return builder.toString();
    }

    public static void main(String[] args) {


    }
}
