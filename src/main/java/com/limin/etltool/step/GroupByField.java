package com.limin.etltool.step;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.limin.etltool.core.Reducer;
import com.limin.etltool.util.Beans;
import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.collections.CollectionUtils;

import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/21
 */
public class GroupByField<E> extends CachedBeanOperationTransform<Collection<E>, Map<Map<String, Object>, List<E>>>
        implements Reducer<E, Collection<E>, Map<Map<String, Object>, List<E>>> {

    private final List<String> fieldNames;

    public GroupByField(String... names) {
        this(Lists.newArrayList(names));
    }

    public GroupByField(List<String> fieldNames) {
        checkArgument(!CollectionUtils.isEmpty(fieldNames), "field names required");
        this.fieldNames = fieldNames;
    }

    private Map<String, Object> extractKeyFrom(E data) {
        Map<String, Object> key = Maps.newHashMap();
        fieldNames.forEach(f -> {
            if (data instanceof Map)
                ((Map) data).put(f, ((Map) data).get(f));
            else {
                Beans.FastBeanOperation op = loadOperation(data);
                key.put(f, op.invokeGetter(data, f));
            }
        });
        return key;
    }

    @Override
    public Map<Map<String, Object>, List<E>> init() {
        return Maps.newLinkedHashMap();
    }

    @Override
    public Map<Map<String, Object>, List<E>> accumulate(Map<Map<String, Object>, List<E>> out, E data) {

        Map<String, Object> key = extractKeyFrom(data);
        out.putIfAbsent(key, Lists.newArrayList());
        out.get(key).add(data);
        return out;
    }
}
