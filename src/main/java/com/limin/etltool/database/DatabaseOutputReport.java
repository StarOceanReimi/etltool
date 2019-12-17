package com.limin.etltool.database;

import com.limin.etltool.core.OutputReport;

public class DatabaseOutputReport implements OutputReport {

    private String table;

    public DatabaseOutputReport(String table) {
        this.table = table;
    }

    @Override
    public void logSuccessResult(Object result) {

    }

    @Override
    public void logErrorResult(Object result) {

    }

    @Override
    public boolean hasError() {
        return false;
    }
}
