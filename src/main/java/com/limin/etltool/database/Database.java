package com.limin.etltool.database;

import com.limin.etltool.database.mysql.ColumnDefinition;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.List;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/18
 */
public interface Database extends DataSource {

    Connection getConnection();

    void setPoolConfig(GenericObjectPoolConfig poolConfig);

    void shutdownPool();

    boolean createTable(String table, String tableComment,
                        List<ColumnDefinition> defs);

    boolean createTable(String table, String tableComment,
                        List<ColumnDefinition> defs,
                        String[] primaryKeys,
                        ColumnDefinition.Index[] indices);

    boolean executeSQL(String ddl);

    void optimizeForBatchWriting();

    DatabaseConfiguration getConfiguration();
}
