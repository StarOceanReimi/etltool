package com.limin.etltool.step;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.limin.etltool.core.Reducer;
import com.limin.etltool.util.Beans;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/21
 */
public class GroupByField<E> extends CachedBeanOperationTransform<Stream<E>, Map<Map<String, Object>, List<E>>>
        implements Reducer<E, Stream<E>, Map<Map<String, Object>, List<E>>> {

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
        Beans.FastBeanOperation op = loadOperation(data);
        fieldNames.forEach(f -> {
            if (data instanceof Map)
                key.put(f, ((Map) data).get(f));
            else {
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
