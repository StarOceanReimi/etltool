package com.limin.etltool.database;

import com.limin.etltool.core.EtlException;
import com.limin.etltool.database.mysql.DefaultMySqlDatabase;
import lombok.val;

import java.util.Map;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/18
 */
public class NormalDbInput<T> extends AbstractDbInput<T> {

    protected NormalDbInput(Database database, DatabaseAccessor accessor) {
        super(null, database, accessor);
    }

    public NormalDbInput(Class<T> componentType, Database database, DatabaseAccessor accessor) {
        super(componentType, database, accessor);
    }

    public static void main(String[] args) throws EtlException {

        DatabaseAccessor accessor =
                new TableColumnAccessor(TableColumnAccessor.SqlType.SELECT, "my_test");
        DatabaseConfiguration configuration = new DatabaseConfiguration();
        Database database = new DefaultMySqlDatabase(configuration);
        val input = new NormalDbInput<Map<String, Object>>(database, accessor) {} ;
        val result = input.readCollection();
        result.forEach(System.out::println);
    }
}
