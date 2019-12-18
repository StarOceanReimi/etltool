package com.limin.etltool.database.mysql;

import com.limin.etltool.database.AbstractJdbcDatabaseSource;
import com.limin.etltool.database.DatabaseConfiguration;
import com.limin.etltool.database.DatabaseSource;

import java.util.List;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/17
 */
public class MySqlDatabaseSource extends AbstractJdbcDatabaseSource {

    public MySqlDatabaseSource(DatabaseConfiguration configuration) {
        super(configuration);
    }

    @Override
    public DatabaseSource configureConnectionProperties(String name, Object value) {

        if(value == null && configuration.getAttributes().containsKey(name)) {
            configuration.getAttributes().remove(name);
        } else if (value != null) {
            configuration.getAttributes().put(name, value);
        }
        return this;
    }

    @Override
    public String getTable() {
        return null;
    }

    @Override
    public List<String> getColumns() {
        return null;
    }
}
