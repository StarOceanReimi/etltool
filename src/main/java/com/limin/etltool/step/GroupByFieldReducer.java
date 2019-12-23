package com.limin.etltool.step;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.limin.etltool.core.Transformer;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/21
 */
public abstract class GroupFieldReducer<E, O>
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
        data.forEach((key, value) -> {
            O out = getOutSupplier().get();
            if(out instanceof Map) ((Map) out).putAll(key);
            else setPropertiesFromMap(key);
            merge(out, reduce(value));
            result.add(out);
        });
        return result;
    }

    private void merge(O out, O reduce) {

    }

    protected void setPropertiesFromMap(Map<String, Object> key) {

    }
}
