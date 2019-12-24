package com.limin.etltool.step;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.limin.etltool.core.Reducer;

import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/23
 */
public class GroupByFieldWithCondition<E>
        extends CachedBeanOperationTransform<Stream<E>, Map<Map<String, Object>, List<E>>>
        implements Reducer<E, Stream<E>, Map<Map<String, Object>, List<E>>> {

    private Map<String, Predicate<E>> predicateMap;

    private Map<String, String> fieldKeyMap;

    private String otherGroupName = "__other__";

    public GroupByFieldWithCondition() {
        this.fieldKeyMap = Maps.newHashMap();
        this.predicateMap = Maps.newHashMap();
    }

    public GroupByFieldWithCondition otherGroupName(String groupName) {
        this.otherGroupName = groupName;
        return this;
    }

    public GroupByFieldWithCondition<E> addMapping(String fieldName, String statName, Predicate<E> predicate) {
        checkArgument(!Strings.isNullOrEmpty(fieldName), "fieldName cannot be empty");
        checkArgument(!Strings.isNullOrEmpty(statName), "statName cannot be empty");
        checkNotNull(predicate, "reducer cannot be null");
        predicateMap.put(fieldName, predicate);
        fieldKeyMap.put(fieldName, statName);
        return this;
    }

    @Override
    public Map<Map<String, Object>, List<E>> accumulate(Map<Map<String, Object>, List<E>> out, E data) {
        Map<String, Object> key = extractKeyFrom(data);
        out.putIfAbsent(key, Lists.newArrayList());
        out.get(key).add(data);
        return out;
    }

    private Map<String, Object> extractKeyFrom(E data) {
        Map<String, Object> key = Maps.newHashMap();
        fieldKeyMap.forEach((name, statName) -> {
            if(predicateMap.get(name).test(data))
                key.put(name, statName);
            else
                key.put(name, otherGroupName);
        });
        return key;
    }

    @Override
    public Map<Map<String, Object>, List<E>> init() {
        return Maps.newLinkedHashMap();
    }
}
