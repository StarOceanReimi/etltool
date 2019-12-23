package com.limin.etltool.step;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.limin.etltool.core.Transformer;
import com.limin.etltool.util.Beans;
import org.apache.commons.beanutils.PropertyUtils;

import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.function.Supplier;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/21
 */
public abstract class GroupByFieldReducer<E, O>
    extends CachedBeanOperationTransform<Map<Map<String, Object>, List<E>>, Collection<O>>
        implements Transformer<Map<Map<String, Object>, List<E>>, Collection<O>> {

    public abstract O reduce(List<E> data);

    @SuppressWarnings("unchecked")
    protected Supplier<O> getOutSupplier() {
        return () -> (O) Maps.newLinkedHashMap();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Collection<O> transform(Map<Map<String, Object>, List<E>> data) {
        Collection<O> result = Lists.newArrayList();
        if(result.isEmpty()) return Collections.emptyList();

        O sampleData = getOutSupplier().get();
        Beans.FastBeanOperation op = loadOperation(sampleData);

        data.forEach((key, value) -> {
            O out = getOutSupplier().get();
            if(out instanceof Map) ((Map) out).putAll(key);
            else setPropertiesFromMap(out, key, op);
            merge(out, reduce(value));
            result.add(out);
        });
        return result;
    }

    @SuppressWarnings("unchecked")
    private void merge(O out, O reduce) {
        if(out instanceof Map && reduce instanceof Map)
            ((Map) out).putAll((Map) reduce);
        else
            Beans.copy(out, reduce);
    }

    protected void setPropertiesFromMap(O out, Map<String, Object> key, Beans.FastBeanOperation op) {
        for (String propName : key.keySet()) {
            op.invokeSetter(out, propName, key.get(propName));
        }
    }
}
