package com.limin.etltool.step;

import com.google.common.collect.Maps;
import com.limin.etltool.core.EtlException;
import com.limin.etltool.core.Transformer;
import com.limin.etltool.database.*;
import com.limin.etltool.database.mysql.DefaultMySqlDatabase;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.beanutils.PropertyUtils;

import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/19
 */
@Slf4j
public class ColumnMapping<T1, T2> implements Transformer<T1, T2> {

    private Map<String, String> columnMapping = Maps.newHashMap();

    private final Supplier<T2> outputBeanSupplier;

    public ColumnMapping() {
        this(null);
    }

    @SuppressWarnings("unchecked")
    public ColumnMapping(Supplier<T2> outputBeanSupplier) {
        if(outputBeanSupplier == null)
            this.outputBeanSupplier = () -> (T2) new HashMap<String, Object>();
        else
            this.outputBeanSupplier = outputBeanSupplier;
    }

    public ColumnMapping<T1, T2> addMapping(String inputColumn, String outputColumn) {
        columnMapping.put(inputColumn, outputColumn);
        return this;
    }

    private void setValueForTarget(Object target, String name, Object value) {
        if(target instanceof Map) {
            Map<String, Object> outputMap = (Map<String, Object>) target;
            outputMap.put(name, value);
        } else {
            try {
                PropertyUtils.setProperty(target, name, value);
            } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
                log.warn("can not set property {} for {}", name, target);
            }
        }
    }

    @Override
    public T2 transform(T1 data) {
        T2 target = outputBeanSupplier.get();
        if(data instanceof Map) {
            Map<String, Object> inputMap = (Map<String, Object>) data;
            for(String input : inputMap.keySet()) {
                String output = columnMapping.get(input);
                Object value = inputMap.get(input);
                setValueForTarget(target, output, value);
            }
        } else {
            PropertyDescriptor[] descs = PropertyUtils.getPropertyDescriptors(data.getClass());
            for(PropertyDescriptor desc : descs) {
                String input = desc.getDisplayName();
                if("class".equals(input)) continue;
                String output = columnMapping.get(input);
                Object value;
                try {
                    Method method = desc.getReadMethod();
                    method.setAccessible(true);
                    value = method.invoke(data);
                } catch (IllegalAccessException | InvocationTargetException e) {
                    log.warn("can not read property {} from {}", input, data);
                    continue;
                }
                setValueForTarget(target, output, value);
            }
        }
        return target;
    }

    public static void main(String[] args) throws EtlException {
        DatabaseConfiguration configuration = new DatabaseConfiguration();
        Database database = new DefaultMySqlDatabase(configuration);
        DatabaseAccessor accessor = new TableColumnAccessor("co_comment");
        DbInput input = new NormalDbInput<Map<String, Object>>(database, accessor) {};
        val collection = input.readCollection();
        collection.forEach(System.out::println);


    }
}
