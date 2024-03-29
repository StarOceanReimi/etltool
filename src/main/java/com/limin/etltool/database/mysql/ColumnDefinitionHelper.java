package com.limin.etltool.database.mysql;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.limin.etltool.database.util.IdKey;
import org.apache.commons.beanutils.PropertyUtils;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.limin.etltool.database.mysql.ColumnDefinition.ColumnType.guessFromName;
import static com.limin.etltool.database.mysql.ColumnDefinition.ColumnType.guessFromType;
import static com.limin.etltool.util.Exceptions.inform;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/19
 */
public abstract class ColumnDefinitionHelper {

    private static final ThreadLocal<Map<String, ColumnDefinition.ColumnType>> nameMapping =
            ThreadLocal.withInitial(Maps::newHashMap);

    private static final ThreadLocal<Map<Class<?>, ColumnDefinition.ColumnType>> typeMapping =
            ThreadLocal.withInitial(Maps::newHashMap);


    public static void suggestColumnType(String name, ColumnDefinition.ColumnType type) {
        checkArgument(!Strings.isNullOrEmpty(name), "name must not be null");
        checkNotNull(type, "type must not be null");
        if (!nameMapping.get().containsKey(name))
            nameMapping.get().put(name, type);
    }

    public static void suggestColumnType(Class<?> classType, ColumnDefinition.ColumnType type) {
        checkNotNull(classType, "classType must not be null");
        checkNotNull(type, "type must not be null");
        if (!typeMapping.get().containsKey(classType))
            typeMapping.get().put(classType, type);
    }

    public static List<ColumnDefinition> fromMap(Map<String, Object> map) {
        return fromMap(map, null);
    }

    private static boolean isPrimaryKey(String propName, Class<?> beanType) {

        if ("id".equalsIgnoreCase(propName)) return true;
        if (beanType != null) {
            try {
                Field field = beanType.getDeclaredField(propName);
                return field.isAnnotationPresent(IdKey.class);
            } catch (NoSuchFieldException e) {
                // swallow it
            }
        }
        return false;
    }

    public static List<ColumnDefinition> fromClass(Class<?> clazz) {
        return fromClass(clazz, null);
    }

    public static List<ColumnDefinition> fromClass(Class<?> clazz, Map<String, ColumnDefinition.ColumnType> typeConfig) {
        PropertyDescriptor[] descriptors = PropertyUtils.getPropertyDescriptors(clazz);
        List<ColumnDefinition> defs = Lists.newArrayList();
        typeConfig = Optional.ofNullable(typeConfig).orElseGet(Maps::newHashMap);
        Map<String, ColumnDefinition.ColumnType> suggests = ImmutableMap
                .<String, ColumnDefinition.ColumnType>builder()
                .putAll(nameMapping.get())
                .putAll(typeConfig)
                .build();
        for (PropertyDescriptor descriptor : descriptors) {
            String propName = descriptor.getDisplayName();
            if (propName.equals("class")) continue;
            Method read = descriptor.getReadMethod();
            ColumnDefinition.ColumnType type = guessFromName(propName);
            if (type == null) type = suggests.get(propName);
            if (type == null) type = guessFromType(read.getReturnType());
            if (type == null)
                throw inform("cannot determine type for property {} in class {}", propName, clazz);

            ColumnDefinition def = ColumnDefinition.builder()
                    .name(propName)
                    .type(type)
                    .primaryKey(isPrimaryKey(propName, clazz))
                    .build();
            defs.add(def);
        }
        return defs;
    }

    public static List<ColumnDefinition> fromMap(Map<String, Object> map, Map<String, ColumnDefinition.ColumnType> typeConfig) {

        List<ColumnDefinition> defs = Lists.newArrayList();

        typeConfig = Optional.ofNullable(typeConfig).orElseGet(Maps::newHashMap);

        Map<String, ColumnDefinition.ColumnType> suggests = ImmutableMap
                .<String, ColumnDefinition.ColumnType>builder()
                .putAll(nameMapping.get())
                .putAll(typeConfig)
                .build();

        for (String propName : map.keySet()) {
            Object value = map.get(propName);

            ColumnDefinition.ColumnType type = guessFromName(propName);
            if (type == null)
                type = suggests.get(propName);
            if (value != null && type == null) {
                type = guessFromType(value.getClass());
                if (type == null)
                    type = typeMapping.get().get(value.getClass());
            }
            if (type == null)
                throw inform("cannot determine type for property {} in map", propName);

            ColumnDefinition def = ColumnDefinition.builder()
                    .name(propName)
                    .type(type)
                    .primaryKey(isPrimaryKey(propName, null))
                    .build();
            defs.add(def);
        }
        return defs;

    }
}
