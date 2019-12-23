package com.limin.etltool.database;

import com.limin.etltool.core.OutputReport;

import java.sql.SQLException;

public class DefaultDatabaseOutputReport implements OutputReport {
    @Override
    public void logSuccessResult(Object result) {

    }

    @Override
    public void logErrorResult(Object result) {

        if(result instanceof SQLException) {
            ((SQLException) result).printStackTrace();
        }

    }

    @Override
    public boolean hasError() {
        return false;
    }
}
