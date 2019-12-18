package com.limin.etltool.database;

import com.google.common.collect.Maps;
import com.limin.etltool.core.Source;
import lombok.Data;

import java.util.Map;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/18
 */
@Data
public class DefaultDatabaseAccessor implements DatabaseAccessor {

    private final String sql;

    private Map<String, Object> params = Maps.newHashMap();

    public DefaultDatabaseAccessor(String sql) {
        this.sql = sql;
    }

    public DefaultDatabaseAccessor param(String name, Object value) {
        if(value == null) {
            if(params.containsKey(name)) params.remove(name);
            return this;
        }
        this.params.put(name, value);
        return this;
    }

    @Override
    public boolean accept(Source source) {
        return (source instanceof DbInput) || (source instanceof DbOutput);
    }
}
