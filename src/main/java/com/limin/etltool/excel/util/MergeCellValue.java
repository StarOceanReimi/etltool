package com.limin.etltool.excel.util;

import com.limin.etltool.excel.annotation.ValueGenerator;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.util.CellAddress;
import org.apache.poi.ss.util.CellRangeAddress;

public class MergeCellValue implements ValueGenerator {

    @Override
    public Object value(Sheet currentSheet, int row, int column) {
        Cell previousCell = currentSheet.getRow(row).getCell(column-1);
        CellAddress address = new CellAddress(row, column);
        CellRangeAddress addresses =
                CellRangeAddress.valueOf(previousCell.getAddress() + ":" + address);
        currentSheet.addMergedRegion(addresses);
        previousCell.setCellValue(previousCell.getStringCellValue() + "_Hello");
        return BLANK_CELL;
    }
}
