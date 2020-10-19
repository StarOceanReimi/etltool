package com.limin.etltool.database;

import com.google.common.reflect.TypeResolver;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static java.util.Optional.ofNullable;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/18
 */
public abstract class DbSupport<T> implements AutoCloseable {

    private static Logger log = LoggerFactory.getLogger(DbSupport.class);

    protected volatile Connection connection;

    protected DatabaseAccessor accessor;

    protected Class<T> componentType;

    protected Database database;

    private static TypeResolver typeResolver = new TypeResolver();

    private ConnectionAliveCheckingThread checkingThread;

    @SuppressWarnings("unchecked")
    public DbSupport(
            Class<T> componentType,
            Database database,
            DatabaseAccessor accessor) {
        Objects.requireNonNull(database);
        Objects.requireNonNull(accessor);
        if (componentType == null) {
            TypeToken<T> typeToken = new TypeToken<T>(getClass()) {
            };
            this.componentType = (Class<T>) typeToken.getRawType();
        } else {
            this.componentType = componentType;
        }
        this.accessor = accessor;
        this.database = database;
        initializeConnection(database);
    }

    public void keepConnectionAlive() {
        keepConnectionAlive(null, null);
    }

    public void keepConnectionAlive(String checkSql, Long checkInterval) {
        if(checkingThread == null) {
            checkingThread = new ConnectionAliveCheckingThread(checkSql, checkInterval);
            checkingThread.start();
        }
    }

    protected void initializeConnection(Database database) {
        connection = database.getConnection();
    }

    @Override
    public void close() throws Exception {
        if (connection != null && !connection.isClosed())
            connection.close();
        if (checkingThread != null) {
            try {
                checkingThread.join();
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
            checkingThread = null;
        }
    }

    private class ConnectionAliveCheckingThread extends Thread {

        private String checkSql;

        private Long checkInterval;

        ConnectionAliveCheckingThread(String checkSql, Long checkInterval) {
            super("Connection-AliveChecking-Thread");
            this.checkSql = ofNullable(checkSql).orElse("select 1");
            this.checkInterval = ofNullable(checkInterval).orElse(5000L);
        }

        @Override
        public void run() {
            try {
                while (true) {
                    if (!(connection != null && !connection.isClosed())) {
                        log.debug("connection is closed. stop checking.");
                        break;
                    }
                    Statement statement = connection.createStatement();
                    ResultSet result = statement.executeQuery(checkSql);
                    if(result.next()) log.trace("connection is alive");
                    result.close();
                    statement.close();
                    Uninterruptibles.sleepUninterruptibly(checkInterval, TimeUnit.MILLISECONDS);
                }
            } catch (SQLException ex) {
                log.warn("sql exception occurs when checking connection availability.", ex);
            }
        }
    }
}
