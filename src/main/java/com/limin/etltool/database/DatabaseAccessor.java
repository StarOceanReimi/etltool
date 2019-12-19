package com.limin.etltool.database;

import com.limin.etltool.core.Source;

import java.util.Map;

/**
 * @author 邱理
 * @description 数据库访问抽象
 * @date 创建于 2019/12/18
 */
public interface DatabaseAccessor {

    /**
     * 例如: select id, name, age from A where name = :name
     * @return 可带参数的SQL
     */
    String getSql(Object bean);

    /**
     * 例如: { name: 'QL' }
     * @return 参数对象
     */
    Map<String, Object> getParams();

    boolean accept(Source source);

    String getInsertedReturnKeyName();
}
