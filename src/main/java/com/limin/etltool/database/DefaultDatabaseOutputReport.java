package com.limin.etltool.database;

import com.limin.etltool.core.OutputReport;

public class DefaultDatabaseOutputReport implements OutputReport {
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
