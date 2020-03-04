package com.limin.etltool.excel.util;

import com.limin.etltool.excel.annotation.CellStyleSetter;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFFont;

import java.util.concurrent.atomic.AtomicReference;

public abstract class CellStyles {


    public static final class FontNormalStyle implements CellStyleSetter {
        AtomicReference<Font> fontRef = new AtomicReference<>(null);
        @Override
        public void applyStyle(Cell cell, CellStyle style) {
            if(fontRef.get() == null) {
                fontRef.compareAndSet(null, cell.getSheet().getWorkbook().createFont());
            }
            Font font = fontRef.get();
            font.setBold(false);
            style.setFont(font);
        }
    }

    public static final class FontBoldStyle implements CellStyleSetter {
        AtomicReference<Font> fontRef = new AtomicReference<>(null);
        @Override
        public void applyStyle(Cell cell, CellStyle style) {
            if(fontRef.get() == null) {
                fontRef.compareAndSet(null, cell.getSheet().getWorkbook().createFont());
            }
            Font font = fontRef.get();
            font.setBold(true);
            style.setFont(font);
        }
    }

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
