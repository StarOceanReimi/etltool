package com.limin.etltool.excel.annotation;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;

public interface CellStyleSetter {

    void applyStyle(Cell cell, CellStyle style);
}
