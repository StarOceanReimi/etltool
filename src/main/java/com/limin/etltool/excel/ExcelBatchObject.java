package com.limin.etltool.excel;

import com.google.common.collect.Lists;
import com.limin.etltool.core.Batch;
import com.limin.etltool.excel.transformer.RowTransformer;
import com.limin.etltool.excel.transformer.RowTransformerFactory;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

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

    private final Class<T> targetClass;

    public ExcelBatchObject(Sheet sheet, int batchSize, Class<T> targetClass) {
        this.batchSize = batchSize;
        this.sheet = sheet;
        this.rowIterator = sheet.iterator();
        this.targetClass = targetClass;
    }

    @Override
    public boolean hasMore() {
        return rowIterator.hasNext();
    }

    @Override
    public List<T> getMore() {

        List<T> result = Lists.newArrayList();
        int count = 0;

        RowTransformer<T> transformer = RowTransformerFactory.getTransformer(targetClass);
        while (rowIterator.hasNext() || count == 0 || count != batchSize) {
            result.add(transformer.transform(rowIterator.next()));
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
