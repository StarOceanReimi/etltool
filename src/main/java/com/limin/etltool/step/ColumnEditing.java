package com.limin.etltool.step;

import com.google.common.collect.Maps;
import com.limin.etltool.core.Transformer;
import com.limin.etltool.util.Beans;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.beanutils.PropertyUtils;

import java.beans.PropertyDescriptor;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/19
 */
@Slf4j
public class ColumnEditing<E> extends CachedBeanOperationTransform<Stream<E>, Stream<E>>
        implements Transformer<Stream<E>, Stream<E>> {

    private Map<String, Consumer<E>> columnEditorMemo = Maps.newHashMap();

    public void registerEditor(String columnName, Consumer<E> editor) {
        columnEditorMemo.put(columnName, editor);
    }

    @SuppressWarnings("unchecked")
    protected E innerTransform(E data) {
        if(data instanceof Map) {
            Map<String, E> dataMap = ((Map<String, E>) data);
            Set<String> keySet = dataMap.keySet();
            for (String key : keySet) {
                Consumer<E> editor = columnEditorMemo.get(key);
                if(editor != null) editor.accept(data);
            }
        } else {
            Beans.FastBeanOperation dataOp = loadOperation(data);
            PropertyDescriptor[] descriptors = PropertyUtils.getPropertyDescriptors(data.getClass());
            for (PropertyDescriptor descriptor : descriptors) {
                String propertyName = descriptor.getDisplayName();
                Consumer<E> editor = columnEditorMemo.get(propertyName);
                if (editor != null) {
                    E value = (E) dataOp.invokeGetter(data, propertyName);
                    editor.accept(value);
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
