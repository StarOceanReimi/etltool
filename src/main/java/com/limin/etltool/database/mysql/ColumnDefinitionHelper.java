package com.limin.etltool.database.mysql;

import com.google.common.collect.Lists;
import com.limin.etltool.util.ReflectionUtils;
import org.apache.commons.beanutils.PropertyUtils;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

import static com.limin.etltool.database.mysql.ColumnDefinition.ColumnType.guessFromName;
import static com.limin.etltool.database.mysql.ColumnDefinition.ColumnType.guessFromType;
import static com.limin.etltool.util.Exceptions.inform;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/19
 */
public abstract class ColumnDefinitionHelper {

    public static List<ColumnDefinition> fromMap(Map<String, Object> map) {
        List<ColumnDefinition> defs = Lists.newArrayList();
        for(String propName : map.keySet()) {
            Object value = map.get(propName);
            ColumnDefinition.ColumnType type = null;
            if(value != null)
                type = guessFromType(value.getClass());
            if(type == null)
                type = guessFromName(propName);
            if(type == null)
                throw inform("cannot determine type for property {} in map", propName);
            ColumnDefinition def = ColumnDefinition.builder()
                    .name(propName)
                    .type(type)
                    .primaryKey(propName.equals("id"))
                    .build();
            defs.add(def);
        }
        return defs;
    }

    public static List<ColumnDefinition> fromClass(Class<?> clazz) {

        PropertyDescriptor[] descriptors = PropertyUtils.getPropertyDescriptors(clazz);
        List<ColumnDefinition> defs = Lists.newArrayList();
        for (PropertyDescriptor descriptor : descriptors) {
            String propName = descriptor.getDisplayName();
            if(propName.equals("class")) continue;
            Method read = descriptor.getReadMethod();
            ColumnDefinition.ColumnType type = guessFromType(read.getReturnType());
            if(type == null) type = guessFromName(propName);
            if(type == null)
                throw inform("cannot determine type for property {} in class {}", propName, clazz);

            ColumnDefinition def = ColumnDefinition.builder()
                    .name(propName)
                    .type(type)
                    .primaryKey(propName.equals("id"))
                    .build();
            defs.add(def);
        }
        return defs;
    }
}
