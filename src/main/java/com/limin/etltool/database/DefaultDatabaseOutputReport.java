package com.limin.etltool.database;

import com.limin.etltool.core.OutputReport;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;

@Slf4j
public class DefaultDatabaseOutputReport implements OutputReport {

    @Override
    public void logSuccessResult(Object result) {

    }

    @Override
    public void logErrorResult(Object result) {
        if(result instanceof SQLException)
            log.error("error: ", (SQLException) result);
    }

    @Override
    public boolean hasError() {
        return false;
    }
}
