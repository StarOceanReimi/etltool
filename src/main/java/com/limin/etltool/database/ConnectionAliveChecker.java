package com.limin.etltool.database;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Uninterruptibles;
import lombok.val;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 *
 * </p>
 *
 * @author 邱理 WHRDD-PC104
 * @since 2020/10/19
 */
public class ConnectionAliveChecker {

    interface ConnectionInvalidCallback {
        void onInvalid(String name, Connection connection);
    }

    private ConcurrentMap<String, Connection> checkerMap;
    private ConcurrentMap<String, ConnectionInvalidCallback> checkerCallbacks;

    private volatile CheckerThread checkerThread;

    private static final Logger log = LoggerFactory.getLogger(ConnectionAliveChecker.class);

    private static class InstanceHolder {
        static ConnectionAliveChecker INSTANCE = new ConnectionAliveChecker();
    }

    public static ConnectionAliveChecker getInstance() {
        return InstanceHolder.INSTANCE;
    }

    static {
        getInstance().start();
    }

    private ConnectionAliveChecker() {
        checkerMap = new ConcurrentHashMap<>();
        checkerCallbacks = new ConcurrentHashMap<>();
    }

    public void register(String name, Connection connection, ConnectionInvalidCallback callback) {
        Objects.requireNonNull(name);
        Objects.requireNonNull(connection);
        try {
            Preconditions.checkState(!connection.isClosed(), "connection cannot be stopped");
        } catch (SQLException ex) {
            //swallow
        }
        checkerMap.put(name, connection);
        if(callback != null)
            checkerCallbacks.put(name, callback);
    }

    public void deRegister(String name) {
        checkerMap.remove(name);
        checkerCallbacks.remove(name);
    }

    public void start() {
        if(checkerThread == null) {
            checkerThread = new CheckerThread();
            checkerThread.start();
        }
    }

    public void stop() {

        if(checkerThread != null) {
            try {
                checkerThread.join();
            } catch (InterruptedException e) {
                log.warn("checker thread was interrupted.", e);
                Thread.currentThread().interrupt();
            }
            checkerThread = null;
        }

    }

    private class CheckerThread extends Thread {

        CheckerThread() {
            super("Connection CheckerThread");
        }

        @Override
        public void run() {

            while (true) {
                val iter = checkerMap.entrySet().iterator();
                while (iter.hasNext()) {
                    Map.Entry<String, Connection> entry = iter.next();
                    if(!check(entry.getValue())) {
                        fireCallbacks(entry.getKey(), entry.getValue());
                        iter.remove();
                        log.warn("{} connection status is not valid any more.", entry.getKey());
                    }
                }
                Uninterruptibles.sleepUninterruptibly(5000, TimeUnit.MILLISECONDS);
            }

        }

        private boolean check(Connection conn) {
            try {
                if(conn.isClosed()) return false;
                Statement stmt = conn.createStatement();
                ResultSet result = stmt.executeQuery("select 1");
                boolean connectionResult = false;
                if(result.next()) connectionResult = true;
                result.close();
                stmt.close();
                return connectionResult;
            } catch (SQLException ex) {
                return false;
            }
        }
    }

    private void fireCallbacks(String key, Connection value) {
        ConnectionInvalidCallback callback = checkerCallbacks.get(key);
        if(callback != null) callback.onInvalid(key, value);
    }

}
