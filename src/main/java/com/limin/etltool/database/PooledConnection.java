package com.limin.etltool.database;

import org.apache.commons.pool2.impl.GenericObjectPool;

import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

/**
 * <p>
 *
 * </p>
 *
 * @author 邱理 WHRDD-PC104
 * @since 2020/6/10
 */
public class PooledConnection implements Connection {

    private final GenericObjectPool<Connection> pool;

    private final Connection innerConnection;

    public PooledConnection(Connection innerConnection, GenericObjectPool<Connection> pool) {
        this.pool = pool;
        this.innerConnection = innerConnection;
    }

    @Override
    public Statement createStatement() throws SQLException {
        return innerConnection.createStatement();
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return innerConnection.prepareStatement(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        return innerConnection.prepareCall(sql);
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        return innerConnection.nativeSQL(sql);
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        innerConnection.setAutoCommit(autoCommit);
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return innerConnection.getAutoCommit();
    }

    @Override
    public void commit() throws SQLException {
        innerConnection.commit();
    }

    @Override
    public void rollback() throws SQLException {
        innerConnection.rollback();
    }

    @Override
    public void close() {
        pool.returnObject(innerConnection);
    }

    @Override
    public boolean isClosed() throws SQLException {
        return innerConnection.isClosed();
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return innerConnection.getMetaData();
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        innerConnection.setReadOnly(readOnly);
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return innerConnection.isReadOnly();
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        innerConnection.setCatalog(catalog);
    }

    @Override
    public String getCatalog() throws SQLException {
        return innerConnection.getCatalog();
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        innerConnection.setTransactionIsolation(level);
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return innerConnection.getTransactionIsolation();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return innerConnection.getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
        innerConnection.clearWarnings();
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return innerConnection.createStatement(resultSetType, resultSetConcurrency);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return innerConnection.prepareStatement(sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return innerConnection.prepareCall(sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return innerConnection.getTypeMap();
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        innerConnection.setTypeMap(map);
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        innerConnection.setHoldability(holdability);
    }

    @Override
    public int getHoldability() throws SQLException {
        return innerConnection.getHoldability();
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        return innerConnection.setSavepoint();
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        return innerConnection.setSavepoint(name);
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        innerConnection.rollback();
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        innerConnection.releaseSavepoint(savepoint);
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return innerConnection.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return innerConnection.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return innerConnection.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return innerConnection.prepareStatement(sql, autoGeneratedKeys);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return innerConnection.prepareStatement(sql, columnIndexes);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        return innerConnection.prepareStatement(sql, columnNames);
    }

    @Override
    public Clob createClob() throws SQLException {
        return innerConnection.createClob();
    }

    @Override
    public Blob createBlob() throws SQLException {
        return innerConnection.createBlob();
    }

    @Override
    public NClob createNClob() throws SQLException {
        return innerConnection.createNClob();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        return innerConnection.createSQLXML();
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        return innerConnection.isValid(timeout);
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        innerConnection.setClientInfo(name, value);
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        innerConnection.setClientInfo(properties);
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        return innerConnection.getClientInfo(name);
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return innerConnection.getClientInfo();
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return innerConnection.createArrayOf(typeName, elements);
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        return innerConnection.createStruct(typeName, attributes);
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        innerConnection.setSchema(schema);
    }

    @Override
    public String getSchema() throws SQLException {
        return innerConnection.getSchema();
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        innerConnection.abort(executor);
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        innerConnection.setNetworkTimeout(executor, milliseconds);
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return innerConnection.getNetworkTimeout();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return innerConnection.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return innerConnection.isWrapperFor(iface);
    }

}
