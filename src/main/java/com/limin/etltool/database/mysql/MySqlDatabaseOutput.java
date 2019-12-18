package com.limin.etltool.database.mysql;

import com.limin.etltool.core.EtlException;
import com.limin.etltool.database.AbstractDatabaseOutput;
import com.limin.etltool.database.DatabaseOutputType;
import com.limin.etltool.database.DatabaseSource;

public class MySqlDatabaseOutput<T> extends AbstractDatabaseOutput<T> {

    public MySqlDatabaseOutput(DatabaseOutputType databaseOutputType) {
        super(databaseOutputType, null);
    }

    public MySqlDatabaseOutput(String table, DatabaseOutputType databaseOutputType, Class<T> componentType) {
        super(databaseOutputType, componentType);
    }

    @Override
    protected void optimizeForWrite(DatabaseSource databaseSource) {
        super.optimizeForWrite(databaseSource);
        databaseSource
                .configureConnectionProperties("rewriteBatchedStatements", true);
    }

    public static void main(String[] args) throws EtlException {

    }
}
