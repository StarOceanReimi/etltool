package com.limin.etltool.database.util;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.limin.etltool.database.util.nameconverter.INameConverter;
import com.limin.etltool.database.util.nameconverter.NameConverter;
import com.limin.etltool.database.util.nameconverter.NopNameConverter;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.beanutils.PropertyUtils;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static com.limin.etltool.util.Exceptions.propagate;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/17
 */
@Slf4j
public abstract class DatabaseUtils {

    private static LoadingCache<Class<?>, INameConverter> converterCache = CacheBuilder.newBuilder()
            .build(CacheLoader.from((key) -> {
                if(!key.isAnnotationPresent(NameConverter.class)) return new NopNameConverter();
                NameConverter converter = key.getAnnotation(NameConverter.class);
                try {
                    return converter.value().newInstance();
                } catch (InstantiationException | IllegalAccessException e) {
                    return new NopNameConverter();
                }
            }));

    @SuppressWarnings("unchecked")
    public static <T> T readObjectFromResultSet(ResultSet resultSet, Class<T> clazz) throws SQLException {

        ResultSetMetaData metaData = resultSet.getMetaData();
        int count = metaData.getColumnCount();
        List<JdbcClassFieldDescriptor> descriptors = Lists.newArrayList();
        for(int i = 1; i <= count; i++) {
            JdbcClassFieldDescriptor descriptor = new JdbcClassFieldDescriptor();
            descriptor.setFieldName(metaData.getColumnLabel(i));
            descriptor.setJdbcType(metaData.getColumnType(i));
            descriptor.setValue(resultSet.getObject(i));
            descriptors.add(descriptor);
        }

        if (Map.class.isAssignableFrom(clazz)) {
            return (T) wrapToMap(descriptors);
        } else if (List.class.isAssignableFrom(clazz)) {
            return (T) wrapToList(descriptors);
        } else if (Set.class.isAssignableFrom(clazz)) {
            return (T) wrapToSet(descriptors);
        } else {
            return wrapToBean(descriptors, clazz);
        }

    }

    private static <T> T wrapToBean(List<JdbcClassFieldDescriptor> descriptors, Class<T> clazz) {

        PropertyDescriptor[] beanDescriptors = PropertyUtils.getPropertyDescriptors(clazz);
        Constructor<T> constructor;
        try {
            constructor = clazz.getConstructor();
        } catch (NoSuchMethodException e) {
            try {
                constructor = clazz.getDeclaredConstructor();
                constructor.setAccessible(true);
            } catch (NoSuchMethodException ex) {
                throw propagate(ex);
            }
        }

        Object bean;
        try {
            bean = constructor.newInstance();
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw propagate(e);
        }

        try {
            INameConverter converter = converterCache.get(clazz);
            Map<String, PropertyDescriptor> descriptorMap =
                    Arrays.stream(beanDescriptors).collect(Collectors.toMap((p) -> converter.rename(p.getDisplayName()), v->v));
            for (JdbcClassFieldDescriptor descriptor : descriptors) {
                PropertyDescriptor beanDescriptor = descriptorMap.get(descriptor.getFieldName());
                if(beanDescriptor == null) continue;
                Method writeMethod = beanDescriptor.getWriteMethod();
                try {
                    writeMethod.invoke(bean, descriptor.getValue());
                } catch (IllegalAccessException | InvocationTargetException e) {
                    log.warn("cannot set property {} of class {}", descriptor.getFieldName(), clazz);
                }
            }
        } catch (ExecutionException e) {
            throw propagate(e);
        }

        return (T) bean;
    }

    private static Set<Object> wrapToSet(List<JdbcClassFieldDescriptor> descriptors) {
        Set<Object> result = Sets.newLinkedHashSet();
        descriptors.stream().map(JdbcClassFieldDescriptor::getValue).forEach(result::add);
        return result;
    }

    private static List<Object> wrapToList(List<JdbcClassFieldDescriptor> descriptors) {
        List<Object> result = Lists.newArrayList();
        descriptors.stream().map(JdbcClassFieldDescriptor::getValue).forEach(result::add);
        return result;
    }

    private static Map<String, Object> wrapToMap(List<JdbcClassFieldDescriptor> descriptors) {
        Map<String, Object> result = Maps.newLinkedHashMap();
        descriptors.forEach(desc -> result.put(desc.getFieldName(), desc.getValue()));
        return result;
    }


    @Data
    private static class JdbcClassFieldDescriptor {

        private String fieldName;

        private int    jdbcType;

        private Object value;
    }
}
