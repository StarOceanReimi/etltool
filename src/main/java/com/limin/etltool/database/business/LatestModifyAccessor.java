package com.limin.etltool.database.business;

import com.limin.etltool.core.Source;
import com.limin.etltool.database.DatabaseAccessor;
import com.limin.etltool.database.DbInput;

import java.util.Map;

import static com.limin.etltool.util.TemplateUtils.logFormat;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/25
 */
public class LatestModifyAccessor implements DatabaseAccessor {

    private final String table;

    private String fieldName = "modify_at";

    private String outputName = "latest_modify";

    public LatestModifyAccessor(String table) {
        this.table = table;
    }

    private static final String SQL_TPL = "SELECT MAX({}) {} from {}";

    @Override
    public String getSql(Object bean) {
        return logFormat(SQL_TPL, fieldName, outputName, table);
    }

    @Override
    public Map<String, Object> getParams() {
        return null;
    }

    @Override
    public boolean accept(Source source) {
        return source instanceof DbInput;
    }

    @Override
    public String getInsertedReturnKeyName() {
        return null;
    }
}
