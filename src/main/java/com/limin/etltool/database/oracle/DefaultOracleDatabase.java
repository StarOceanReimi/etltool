package com.limin.etltool.database.oracle;

import com.limin.etltool.database.AbstractDatabase;
import com.limin.etltool.database.DatabaseConfiguration;
import com.limin.etltool.database.mysql.ColumnDefinition;

import java.util.List;

/**
 * <p>
 *
 * </p>
 *
 * @author 邱理 WHRDD-PC104
 * @since 2021/11/5
 */
public class DefaultOracleDatabase extends AbstractDatabase {

    public DefaultOracleDatabase(DatabaseConfiguration configuration) {
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
}
