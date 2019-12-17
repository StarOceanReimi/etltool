package com.limin.etltool.util;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Streams;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.beanutils.ConvertUtils;
import org.apache.commons.collections.MapUtils;

import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;


/**
 * @author 邱理
 * @description
 * @date 创建于 2019/9/4
 */
@Slf4j
public abstract class QueryStringUtils {

    public static Map<String, List<String>> wrapToQueryStringMap(Map<String, Object> map) {
        return wrapToQueryStringMap(map, false);
    }

    @SuppressWarnings("unchecked")
    public static Map<String, List<String>> wrapToQueryStringMap(Map<String, Object> map, boolean convert) {

        if(MapUtils.isEmpty(map)) return null;

        return map.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, (entry)-> {
            Object value = entry.getValue();
            if(Objects.isNull(value)) return singletonList("");
            if(value instanceof Collection) {
                List<String> stringList = null;
                if(convert)
                    stringList = (List<String>) ((Collection) value).stream()
                            .map(ConvertUtils::convert)
                            .collect(Collectors.toList());
                else
                    stringList = Lists.newArrayList((Collection) value);
                return stringList;
            } else {
                if(!(value instanceof String)) {
                    return singletonList(ConvertUtils.convert(value));
                }
                return singletonList((String) value);
            }
        }, (u,v) -> {
            throw new IllegalStateException(String.format("Duplicate key %s", u));
        }, LinkedHashMap::new));
    }

    public static String toQueryString(Map<String, List<String>> queryStringMap) {

        if(MapUtils.isEmpty(queryStringMap)) return "";

        BiFunction<StringBuilder, Map.Entry<String, List<String>>, StringBuilder> accumulator = (buf, entry) ->
            buf.append(entry.getValue().stream().reduce(new StringBuilder(), (sb, value) -> {
                return sb.append("&").append(entry.getKey()).append("=").append(value);
        }, StringBuilder::append));

        return queryStringMap
                .entrySet()
                .stream()
                .reduce(new StringBuilder(), accumulator, StringBuilder::append)
                .substring(1);
    }

    private static Splitter QUERY_STRING_SPLITTER = Splitter.on("&").trimResults();
    private static Splitter QUERY_STRING_VALUE_SPLITTER = Splitter.on("=").trimResults();

    @SuppressWarnings("unchecked")
    public static Map<String, List<String>> fromQueryString(String queryString) {
        val iter = QUERY_STRING_SPLITTER.split(queryString);
        return Streams.stream(iter).reduce(new LinkedHashMap(), (map, keyValuePair)->{
            val list = QUERY_STRING_VALUE_SPLITTER.splitToList(keyValuePair);
            if(list.size() < 1) return map;
            map.putIfAbsent(list.get(0), Lists.newLinkedList());
            List<String> values = (List<String>) map.get(list.get(0));
            if(list.size() < 2) values.add("");
            else values.add(list.get(1));
            return map;
        }, (m1, m2)-> { m1.putAll(m2); return m1; });
    }

    public static void main(String[] args) {
        Map<String, Object> map = Maps.toMap(Arrays.asList("a", "b", "c"), (key)->{
            if("a".equals(key)) return Arrays.asList("a1", "a2", "a3");
            return key+"1";
        });
        System.out.println(map);
        String queryString = toQueryString(wrapToQueryStringMap(map));
        System.out.println(queryString);
        System.out.println(fromQueryString(queryString));

        Map<String, Object> map1 = Maps.toMap(Arrays.asList("a", "b", "c"), (key)->1L);
        System.out.println(map1);
        System.out.println(wrapToQueryStringMap(map1));
        System.out.println(toQueryString(wrapToQueryStringMap(Maps.newHashMap())));

    }

}
