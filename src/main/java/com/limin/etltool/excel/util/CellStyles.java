package com.limin.etltool.excel.util;

import com.limin.etltool.excel.annotation.CellStyleSetter;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.HorizontalAlignment;
import org.apache.poi.ss.usermodel.VerticalAlignment;

public abstract class CellStyles {

    public static final class CenterAlignmentStyle implements CellStyleSetter {
        @Override
        public void applyStyle(Cell cell, CellStyle style) {
            style.setAlignment(HorizontalAlignment.CENTER);
            style.setVerticalAlignment(VerticalAlignment.CENTER);
        }
    }


    public static class WiderLengthStyle implements CellStyleSetter {

        @Override
        public void applyStyle(Cell cell, CellStyle style) {
            cell.getSheet().setColumnWidth(cell.getColumnIndex(), 12 * 256);
        }
    }
}
