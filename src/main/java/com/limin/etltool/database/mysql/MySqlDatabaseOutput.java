package com.limin.etltool.database.mysql;

import com.google.common.collect.Maps;
import com.limin.etltool.core.EtlException;
import com.limin.etltool.core.Output;
import com.limin.etltool.database.AbstractDatabaseOutput;
import com.limin.etltool.database.DatabaseConfiguration;
import com.limin.etltool.database.DatabaseOutputType;
import com.limin.etltool.database.DatabaseSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

public class MySqlDatabaseOutput<T> extends AbstractDatabaseOutput<T> {

    public MySqlDatabaseOutput(String table, DatabaseOutputType databaseOutputType) {
        super(table, databaseOutputType, null);
    }

    public MySqlDatabaseOutput(String table, DatabaseOutputType databaseOutputType, Class<T> componentType) {
        super(table, databaseOutputType, componentType);
    }

    public static void main(String[] args) throws EtlException {

        DatabaseConfiguration configuration = new DatabaseConfiguration();
        configuration.setUrl("jdbc:mysql://localhost:3306/hierarchical_data_exec");
        configuration.setUsername("root");
        configuration.setPassword("db0723..");
        configuration.setDriverClassName("com.mysql.jdbc.Driver");
        configuration
                .attribute("serverTimezone", "Asia/Shanghai")
                .attribute("useUnicode", true)
                .attribute("characterEncoding", "utf-8")
                .attribute("useSSL", false)
                .attribute("allowMultiQueries", true);

        DatabaseSource source = new MySqlDatabaseSource(configuration);
        Output<Map<String, Object>> output = new MySqlDatabaseOutput<>("my_test", new DatabaseOutputType.Update("id", Arrays.asList("name")));
        Map<String, Object> map = Maps.newHashMap();
        map.put("id", 123L);
        map.put("name", "QL1");

        output.writeCollection(Collections.singletonList(map), source);
    }
}
