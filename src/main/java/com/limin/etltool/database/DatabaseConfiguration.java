package com.limin.etltool.database;

import com.google.common.collect.Maps;
import lombok.Data;

import java.util.Map;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/17
 */
@Data
public class DatabaseConfiguration {

    private String url;

    private String username;

    private String password;

    private String driverClassName;

    private Map<String, Object> attributes = Maps.newHashMap();

    public DatabaseConfiguration attribute(String name, Object value) {
        attributes.put(name, value);
        return this;
    }
}
