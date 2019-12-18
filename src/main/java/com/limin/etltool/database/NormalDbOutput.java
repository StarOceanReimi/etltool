package com.limin.etltool.database;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.limin.etltool.core.EtlException;
import com.limin.etltool.database.mysql.DefaultMySqlDatabase;
import com.limin.etltool.database.util.IdKey;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.val;

import java.util.List;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/18
 */
public class NormalDbOutput<T> extends AbstractDbOutput<T> {

    protected NormalDbOutput(Database database, DatabaseAccessor accessor) {
        super(null, database, accessor);
    }

    public NormalDbOutput(Class<T> componentType, Database database, DatabaseAccessor accessor) {
        super(componentType, database, accessor);
    }

    @Data
    @AllArgsConstructor
    public static class TestBean {

        @IdKey
        private String id;

        private String name;

    }

    public static void main(String[] args) throws EtlException {

        DatabaseConfiguration configuration = new DatabaseConfiguration();
        Database database = new DefaultMySqlDatabase(configuration);
        DatabaseAccessor accessor = new TableColumnAccessor(TableColumnAccessor.SqlType.INSERT, "my_test")
                .column("name");
        NormalDbOutput<TestBean> output = new NormalDbOutput<TestBean>(database, accessor) {};
        List<TestBean> mapList = Lists.newArrayList();
        for(int i=0; i<100; i++) {
            mapList.add(new TestBean(null, "QL_" + String.format("%02d", i)));
        }
        val sw = Stopwatch.createStarted();
        output.writeCollection(mapList);
        System.out.println(mapList.get(5).getId());
        System.out.println(sw.stop());

    }
}
