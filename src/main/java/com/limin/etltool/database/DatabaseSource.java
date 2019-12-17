package com.limin.etltool.database;

import com.limin.etltool.core.Source;

import java.sql.Connection;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/16
 */
public interface DatabaseSource extends Source {


    /**
     * 获取数据库连接
     * @return 数据库连接
     */
    Connection getConnection();
}
