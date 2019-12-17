package com.limin.etltool.database.util;

import com.google.common.collect.Lists;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.beanutils.PropertyUtils;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;

@AllArgsConstructor
@Slf4j
public class JdbcSqlParamObject {

    private String jdbcSql;

    private String[] paramNames;

    public String getJdbcSql() {
        return jdbcSql;
    }

    public Object[] buildParam(Object param) {
        List<Object> result = Lists.newArrayList();
        for (String paramName : paramNames) {
            result.add(getVal(paramName, param));
        }
        return result.toArray();
    }

    private Object getVal(String paramName, Object param) {
        if(param instanceof Map) return ((Map) param).get(paramName);
        try {
            return PropertyUtils.getProperty(param, paramName);
        } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            log.warn("cannot fetch property[{}] of class {}", paramName, param.getClass());
            return null;
        }
    }
}
