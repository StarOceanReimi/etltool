package com.limin.etltool.excel.annotation;

import org.apache.poi.ss.usermodel.Sheet;

public interface ValueGenerator {

    Object BLANK_CELL = new Object();

    /**
     * 若返回 @{{@link ValueGenerator#BLANK_CELL}} 那么将不会设置单元格的值
     */
    Object value(Sheet currentSheet, int row, int column);
}
