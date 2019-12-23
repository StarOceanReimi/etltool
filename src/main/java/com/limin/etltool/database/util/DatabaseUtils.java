package com.limin.etltool.database.util;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.limin.etltool.database.util.nameconverter.INameConverter;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.beanutils.ConvertUtils;
import org.apache.commons.beanutils.Converter;
import org.apache.commons.beanutils.PropertyUtils;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.limin.etltool.util.Exceptions.propagate;
import static java.util.stream.Collectors.toMap;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/17
 */
@Slf4j
public abstract class DatabaseUtils {

    private static LoadingCache<Class<?>, INameConverter> converterCache = CacheBuilder.newBuilder()
            .build(CacheLoader.from(INameConverter::getConverter));

    private static final Pattern PARAMS_PATTERN = Pattern.compile("(:\\w+)");

    public static JdbcSqlParamObject buildSqlParamObject(String sqlTemplate) {
        Matcher matcher = PARAMS_PATTERN.matcher(sqlTemplate);
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
            Map<String, PropertyDescriptor> descriptorMap = Arrays.stream(beanDescriptors)
                            .collect(toMap((p) -> converter.rename(p.getDisplayName()), v->v));
            for (JdbcClassFieldDescriptor descriptor : descriptors) {
                PropertyDescriptor beanDescriptor = descriptorMap.get(descriptor.getFieldName());
                if(beanDescriptor == null) continue;
                Method writeMethod = beanDescriptor.getWriteMethod();
                try {
                    Class<?> paramType = writeMethod.getParameterTypes()[0];
                    if(descriptor.getValue() != null) {
                        Object value = descriptor.getValue();
                        Class<?> realType = value.getClass();
                        if(!paramType.isAssignableFrom(realType)) {
                            Converter c = ConvertUtils.lookup(realType, paramType);
                            if(c == null) {
                                log.warn("cannot convert {} to {} for property {} in class {}",
                                        realType, paramType, descriptor.getFieldName(), clazz);
                                continue;
                            }
                            value = c.convert(paramType, value);
                        }
                        writeMethod.invoke(bean, value);
                    }

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

    public static void setParameters(PreparedStatement statement, Object[] params) throws SQLException {
        if(params == null || params.length == 0) return;
        for(int i = 1; i <= params.length; i++)
            statement.setObject(i, params[i - 1]);
    }

    @Data
    private static class JdbcClassFieldDescriptor {

        private String fieldName;

        private int    jdbcType;

        private Object value;
    }
}
