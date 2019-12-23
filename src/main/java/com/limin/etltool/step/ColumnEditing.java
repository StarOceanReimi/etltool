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

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/19
 */
@Slf4j
public class ColumnEditing<I> extends CachedBeanOperationTransform<I, I> implements Transformer<I, I> {

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
            Beans.FastBeanOperation dataOp = loadOperation(data);
            PropertyDescriptor[] descriptors = PropertyUtils.getPropertyDescriptors(data.getClass());
            for (PropertyDescriptor descriptor : descriptors) {
                String propertyName = descriptor.getDisplayName();
                Consumer<I> editor = columnEditorMemo.get(propertyName);
                if (editor != null) {
                    I value = (I) dataOp.invokeGetter(data, propertyName);
                    editor.accept(value);
                }
            }

        }
        return data;
    }
}
