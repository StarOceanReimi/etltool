package com.limin.etltool.excel;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.limin.etltool.core.Batch;
import com.limin.etltool.excel.transformer.RowTransformer;
import com.limin.etltool.excel.transformer.RowTransformerFactory;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.util.CellRangeAddress;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * <p>
 *
 * </p>
 *
 * @author 邱理 WHRDD-PC104
 * @since 2020/1/20
 */
class ExcelBatchObject<T> implements Batch<T> {

    private final int batchSize;

    private final Sheet sheet;

    private final Iterator<Row> rowIterator;

    private GeneralBeanExcelDescriber<T> describer;

    private Map<Cell, Cell> mergeContext;

    public ExcelBatchObject(Sheet workingSheet,
                            Iterator<Row> iterator,
                            int batchSize,
                            GeneralBeanExcelDescriber<T> describer) {
        this.sheet = workingSheet;
        this.rowIterator = iterator;
        this.batchSize = batchSize;
        this.describer = describer;
        this.buildMergeContext();
    }

    private void buildMergeContext() {
        mergeContext = Maps.newHashMap();
        for (CellRangeAddress rangeAddress : sheet.getMergedRegions()) {
            Cell valueCell = sheet.getRow(rangeAddress.getFirstRow()).getCell(rangeAddress.getFirstColumn());
            rangeAddress.forEach(cellAddress -> {
                Cell cell = sheet.getRow(cellAddress.getRow()).getCell(cellAddress.getColumn());
                mergeContext.put(cell, valueCell);
            });
        }
    }

    @Override
    public boolean hasMore() {
        return rowIterator.hasNext();
    }

    @Override
    public List<T> getMore() {

        List<T> result = Lists.newArrayList();
        int count = 0;
        while (rowIterator.hasNext() && (count == 0 || count != batchSize)) {
            result.add(describer.newBean(rowIterator.next(), mergeContext));
            count++;
        }
        return result;
    }

    @Override
    public void release() {

        try {
            sheet.getWorkbook().close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
