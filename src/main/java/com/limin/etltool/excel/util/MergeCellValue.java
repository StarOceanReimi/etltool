package com.limin.etltool.excel.util;

import com.limin.etltool.excel.annotation.ValueGenerator;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.util.CellRangeAddress;

import static java.util.Optional.ofNullable;

public class MergeCellValue implements ValueGenerator {

    @Override
    public Object value(Cell cell, Object data) {
        Cell previousCell = cell.getRow().getCell(cell.getColumnIndex() - 1);
        CellRangeAddress addresses =
                CellRangeAddress.valueOf(previousCell.getAddress() + ":" + cell.getAddress());
        cell.getSheet().addMergedRegion(addresses);
        previousCell.setCellValue(previousCell.getStringCellValue() + ofNullable(data).orElse("_Hello"));
        return BLANK_CELL;
    }
}
