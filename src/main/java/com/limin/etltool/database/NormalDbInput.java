package com.limin.etltool.database;

import com.limin.etltool.core.EtlException;
import com.limin.etltool.database.mysql.DefaultMySqlDatabase;
import com.limin.etltool.database.util.nameconverter.CamelCaseNameConverter;
import com.limin.etltool.database.util.nameconverter.NameConverter;
import lombok.Data;
import lombok.val;

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

    @NameConverter(CamelCaseNameConverter.class)
    @Data
    public static class TestBean {

        private Long parentId;

        private Long unitId;

        private String body;
    }

    public static void main(String[] args) throws EtlException {

        DatabaseAccessor accessor =
                new TableColumnAccessor(TableColumnAccessor.SqlType.SELECT, "co_comment");
        DatabaseConfiguration configuration = new DatabaseConfiguration();
        Database database = new DefaultMySqlDatabase(configuration);
        val input = new NormalDbInput<TestBean>(database, accessor) {} ;
        val result = input.readCollection();
        result.forEach(System.out::println);
    }
}
