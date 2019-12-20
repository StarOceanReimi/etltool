package com.limin.etltool.step;

import com.google.common.collect.Maps;
import com.limin.etltool.core.Transformer;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.beanutils.PropertyUtils;

import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static java.util.Optional.ofNullable;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/19
 */
@Slf4j
public class ColumnEditing<I> implements Transformer<I, I> {

    private Map<String, Consumer<I>> columnEditorMemo = Maps.newHashMap();

    public void registerEditor(String columnName, Consumer<I> editor) {
        columnEditorMemo.put(columnName, editor);
    }

    @SuppressWarnings("unchecked")
    @Override
    public I transform(I data) {
        if(data instanceof Map) {
            Map<String, I> dataMap = ((Map<String, I>) data);
            Set<String> keySet = dataMap.keySet();
            for (String key : keySet) {
                Consumer<I> editor = columnEditorMemo.get(key);
                if(editor != null) editor.accept(data);
            }
        } else {
            PropertyDescriptor[] descriptors = PropertyUtils.getPropertyDescriptors(data.getClass());
            for (PropertyDescriptor descriptor : descriptors) {
                String propertyName = descriptor.getDisplayName();
                Method method = descriptor.getReadMethod();
                Consumer<I> editor = columnEditorMemo.get(propertyName);
                try {
                    if (editor != null) {
                        I value = (I) method.invoke(data);
                        editor.accept(value);
                    }
                } catch (IllegalAccessException | InvocationTargetException e) {
                    log.warn("cannot invoker get property from bean: {}", data);
                }
            }

        }
        return data;
    }
}
