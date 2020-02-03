package com.limin.etltool.excel.util;

import com.limin.etltool.excel.annotation.ValueGenerator;
import org.apache.poi.ss.usermodel.Sheet;

import java.util.concurrent.atomic.AtomicInteger;

public class SerialNoGenerator implements ValueGenerator {

    private AtomicInteger integer = new AtomicInteger(0);

    @Override
    public Object value(Sheet currentSheet, int row, int column) {
        return integer.incrementAndGet();
    }
}
