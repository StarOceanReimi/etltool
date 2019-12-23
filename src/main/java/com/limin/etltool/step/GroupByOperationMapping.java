package com.limin.etltool.step;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.limin.etltool.util.Beans;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/23
 */
public class GroupByOperationMapping<E, O> extends GroupByFieldReducer<E, O> {

    private final Map<String, Collector> mapping;

    private final Map<String, String> statNameMapping;

    private Supplier<?> nullValueHandler;

    public GroupByOperationMapping() {
        mapping = Maps.newHashMap();
        statNameMapping = Maps.newHashMap();
    }

    public GroupByOperationMapping nullValueHandler(Supplier<?> defaultHandler) {
        nullValueHandler = defaultHandler;
        return this;
    }

    public GroupByOperationMapping addMapping(String fieldName, String statName, Collector<E, ?, ?> reducer) {
        checkArgument(!Strings.isNullOrEmpty(fieldName), "fieldName cannot be empty");
        checkArgument(!Strings.isNullOrEmpty(statName), "statName cannot be empty");
        checkNotNull(reducer, "reducer cannot be null");
        mapping.put(fieldName, reducer);
        statNameMapping.put(fieldName, statName);
        return this;
    }

    private Function<E, Object> extractField(final String propName, final Beans.FastBeanOperation op) {
        return (data) -> {
            if(data instanceof Map) return ((Map) data).get(propName);
            else return op.invokeGetter(data, propName);
        };
    }

    @SuppressWarnings("unchecked")
    @Override
    public O reduce(List<E> data) {
        O out = getOutSupplier().get();
        Optional<E> sample = data.stream().findAny();
        if(!sample.isPresent()) return out;
        Beans.FastBeanOperation op = loadOperation(sample.get());
        Map<String, Object> groupedResult = Maps.newHashMap();
        for (String key : mapping.keySet()) {
            Stream<Object> stream = data.stream().map(extractField(key, op));
            if(nullValueHandler == null) stream = stream.filter(Objects::nonNull);
            else stream = stream.map(v -> Objects.isNull(v) ? nullValueHandler.get() : v);
            Object groupedValue = stream.collect(mapping.get(key));
            groupedResult.put(statNameMapping.get(key), groupedValue);
        }
        if(out instanceof Map) ((Map) out).putAll(groupedResult);
        else setPropertiesFromMap(out, groupedResult, op);
        return out;
    }
}
