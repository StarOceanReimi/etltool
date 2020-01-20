package com.limin.etltool.excel;

import com.limin.etltool.core.Batch;
import com.limin.etltool.core.BatchInput;
import com.limin.etltool.core.EtlException;
import com.limin.etltool.util.Exceptions;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;

import java.io.IOException;
import java.io.InputStream;

/**
 * <p>
 *
 * </p>
 *
 * @author 邱理 WHRDD-PC104
 * @since 2020/1/20
 */
public class ExcelInput<T> implements BatchInput<T> {

    private final InputStream inputStream;

    public ExcelInput(InputStream stream) {
        this.inputStream = stream;
        init();
    }

    private void init() {
        Workbook workbook;
        try {
            workbook = WorkbookFactory.create(inputStream);
        } catch (IOException e) {
            throw Exceptions.propagate(e);
        }
    }

    @Override
    public Batch<T> readInBatch(int batchSize) throws EtlException {
        return null;
    }

    public static void main(String[] args) throws IOException {


    }
}























