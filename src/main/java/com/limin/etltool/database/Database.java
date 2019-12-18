package com.limin.etltool.database;

import java.sql.Connection;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/18
 */
public interface Database {

    Connection getConnection();
}
