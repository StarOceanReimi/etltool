package com.limin.etltool.database.mysql;

import com.limin.etltool.database.AbstractJdbcDatabaseSource;
import com.limin.etltool.database.DatabaseConfiguration;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/17
 */
public class MySqlDatabaseSource extends AbstractJdbcDatabaseSource {

    public MySqlDatabaseSource(DatabaseConfiguration configuration) {
        super(configuration);
    }
}
