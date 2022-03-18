package com.limin.etltool.step;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.limin.etltool.util.Beans;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.beanutils.PropertyUtils;

import java.beans.PropertyDescriptor;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/19
 */
@Slf4j
public class ColumnEditing<E> extends CachedBeanOperationTransform<Stream<E>, Stream<E>> {

    private Map<String, Function<?, ?>> columnEditorMemo = Maps.newHashMap();

    public <T, R> void registerEditor(String columnName, Function<T, R> editor) {
        columnEditorMemo.put(columnName, editor);
    }

    @SuppressWarnings("unchecked")
    protected E innerTransform(E data) {
        if (data instanceof Map) {
            Map<String, Object> dataMap = ((Map<String, Object>) data);
            Set<String> keySet = dataMap.keySet();
            final List<Map.Entry<String, Object>> change = Lists.newArrayList();
            for (String key : keySet) {
                Function<?, ?> editor = columnEditorMemo.get(key);
                if (editor != null) {
                    Object d = dataMap.get(key);
                    change.add(new AbstractMap.SimpleEntry<>(key, ((Function<Object, Object>) editor).apply(d)));
                }
            }

            try {
                change.forEach(e -> dataMap.put(e.getKey(), e.getValue()));
            } catch (UnsupportedOperationException ex) {
                Map<String, Object> mutableMap = new HashMap<>(dataMap);
                change.forEach(e -> mutableMap.put(e.getKey(), e.getValue()));
                data = (E) mutableMap;
            }

        } else {
            Beans.FastBeanOperation dataOp = loadOperation(data);
            PropertyDescriptor[] descriptors = PropertyUtils.getPropertyDescriptors(data.getClass());
            for (PropertyDescriptor descriptor : descriptors) {
                String propertyName = descriptor.getDisplayName();
                Function<?, ?> editor = columnEditorMemo.get(propertyName);
                if (editor != null) {
                    Object value = dataOp.invokeGetter(data, propertyName);
                    dataOp.invokeSetter(data, propertyName, ((Function<Object, Object>) editor).apply(value));
                }
            }
        }
        return data;
    }

    @Override
    public Stream<E> transform(Stream<E> data) {
        return data.map(this::innerTransform);
    }
}
