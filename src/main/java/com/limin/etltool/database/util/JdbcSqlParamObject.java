package com.limin.etltool.database.util;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.limin.etltool.util.Beans;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.beanutils.PropertyUtils;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@RequiredArgsConstructor
@Slf4j
public class JdbcSqlParamObject {

    private final String jdbcSql;

    private final String[] paramNames;

    public String getJdbcSql() {
        return jdbcSql;
    }

    private Cache<Class<?>, Beans.FastBeanOperation> operationCache = CacheBuilder.newBuilder().build();

    public Object[] buildParam(Object param) {
        List<Object> result = Lists.newArrayList();
        for (String paramName : paramNames) {
            result.add(getVal(paramName, param));
        }
        return result.toArray();
    }

    private Object getVal(String paramName, Object param) {
        if (param instanceof Map) return ((Map) param).get(paramName);
        try {
            Beans.FastBeanOperation operation =
                    operationCache.get(param.getClass(), () -> Beans.getFastBeanOperation(param.getClass()));
            return operation.invokeGetter(param, paramName);
        } catch (ExecutionException e) {
            log.warn("cannot read cache of {}", param.getClass());
            return null;
        }
    }
}
