package com.limin.etltool.database;

import java.sql.Statement;

/**
 * <p>
 *
 * </p>
 *
 * @author 邱理 WHRDD-PC104
 * @since 2021/4/6
 */
@FunctionalInterface
public interface StatementCallback {

    void doWithStatement(boolean success, Statement statement);
}
