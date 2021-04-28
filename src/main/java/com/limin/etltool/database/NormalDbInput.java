package com.limin.etltool.database;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.limin.etltool.core.EtlException;
import com.limin.etltool.database.mysql.ColumnDefinition;
import com.limin.etltool.database.mysql.ColumnDefinitionHelper;
import com.limin.etltool.database.mysql.DefaultMySqlDatabase;
import lombok.val;
import net.sf.jsqlparser.expression.DateTimeLiteralExpression;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;

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

        String sql = "select * from fact_user limit 10";

        DatabaseAccessor accessor = new DefaultDatabaseAccessor(sql);
//                new LatestModifyAccessor("co_comment");
//                new TableColumnAccessor(TableColumnAccessor.SqlType.SELECT, "my_test");
        DatabaseConfiguration configuration = new DatabaseConfiguration("classpath:database1.yml");
        Database database = new DefaultMySqlDatabase(configuration);
        NormalDbInput<Map<String, Object>> input = new NormalDbInput<Map<String, Object>>(database, accessor) {} ;
        Collection<Map<String, Object>> result = input.readCollection();
        Map<String, Object> sample = result.iterator().next();
        sample = Maps.filterValues(sample, Objects::nonNull);

        Map<String, ColumnDefinition.ColumnType> config = ImmutableMap.<String, ColumnDefinition.ColumnType>builder()
                .put("sex", ColumnDefinition.VARCHAR(5))
                .build();
        System.out.println(ColumnDefinitionHelper.fromMap(sample, config));

    }
}
