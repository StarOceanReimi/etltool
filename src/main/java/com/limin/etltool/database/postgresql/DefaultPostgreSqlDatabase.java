package com.limin.etltool.database.postgresql;

import com.limin.etltool.database.AbstractDatabase;
import com.limin.etltool.database.DatabaseConfiguration;
import com.limin.etltool.database.mysql.ColumnDefinition;
import com.limin.etltool.database.util.DatabaseUtils;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.List;
import java.util.Map;

/**
 * <p>
 *
 * </p>
 *
 * @author 邱理 WHRDD-PC104
 * @since 2021/5/11
 */
public class DefaultPostgreSqlDatabase extends AbstractDatabase {

    public DefaultPostgreSqlDatabase(DatabaseConfiguration configuration) {
        super(configuration);
    }

    @Override
    public boolean createTable(String table, String tableComment, List<ColumnDefinition> defs) {
        throw new RuntimeException("not implemented");
    }

    @Override
    public boolean createTable(String table, String tableComment, List<ColumnDefinition> defs, String[] primaryKeys, ColumnDefinition.Index[] indices) {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void optimizeForBatchWriting() {
    }

    public static void main(String[] args) {

        DatabaseConfiguration configuration = new DatabaseConfiguration("classpath:materialize.yml");
        DefaultPostgreSqlDatabase database = new DefaultPostgreSqlDatabase(configuration);
        database.executeSQL("select * from t_view", (result, stmt) -> {
            if (!result) {
                System.err.println("Error Occurs");
            }
            ResultSet rs = stmt.getResultSet();
            while (rs.next()) {
                System.out.println(DatabaseUtils.readObjectFromResultSet(rs, Map.class));
            }
            rs.close();
        });


    }

}
