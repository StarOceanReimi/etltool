package com.limin.etltool.excel;

import com.google.common.collect.Sets;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.xssf.eventusermodel.XSSFReader;
import org.apache.poi.xssf.eventusermodel.XSSFSheetXMLHandler;
import org.apache.poi.xssf.usermodel.XSSFComment;

import java.io.IOException;
import java.util.Set;

public class ExcelContentHandler extends XSSFSheetXMLHandler {

    private final Set<CellRangeAddress> mergedCells;

    private static class ExcelSheetContentHandler<T> implements XSSFSheetXMLHandler.SheetContentsHandler {

        private final GeneralBeanExcelDescriber<T> describer;

        ExcelSheetContentHandler(GeneralBeanExcelDescriber<T> describer) {
            this.describer = describer;
        }

        @Override
        public void startRow(int rowNum) {

        }

        @Override
        public void endRow(int rowNum) {

        }

        @Override
        public void cell(String cellReference, String formattedValue, XSSFComment comment) {

//            System.out.printf("%s = %s\n", cellReference, formattedValue);

        }
    }

    private static class ExcelDataFormatter extends DataFormatter {
    }

    <T> ExcelContentHandler(
            XSSFReader reader,
            GeneralBeanExcelDescriber<T> describer) throws IOException, InvalidFormatException {
        super(reader.getStylesTable(), null, reader.getSharedStringsTable(),
                new ExcelSheetContentHandler<>(describer), new ExcelDataFormatter(), false);
        this.mergedCells = Sets.newHashSet();
    }

}
