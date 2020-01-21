package com.limin.etltool.excel;

import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.limin.etltool.core.Batch;
import com.limin.etltool.core.BatchInput;
import com.limin.etltool.core.EtlException;
import com.limin.etltool.excel.annotation.Column;
import com.limin.etltool.excel.annotation.HeaderInfo;
import com.limin.etltool.excel.annotation.WorkSheet;
import com.limin.etltool.util.Exceptions;
import com.limin.etltool.util.JavaTimeConverters;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.val;
import org.apache.commons.beanutils.ConvertUtils;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Iterator;

import static com.limin.etltool.util.ReflectionUtils.findGenericTypeFromSuperClass;

/**
 * <p>
 *
 * </p>
 *
 * @author 邱理 WHRDD-PC104
 * @since 2020/1/20
 */
public class ExcelInput<T> implements BatchInput<T> {

    static {
        ConvertUtils.register(JavaTimeConverters.LocalDateTimeInstance, LocalDateTime.class);
        ConvertUtils.register(JavaTimeConverters.LocalDateInstance, LocalDate.class);
    }

    private final Workbook workbook;

    private final GeneralBeanExcelDescriber<T> describer;

    protected ExcelInput(InputStream stream) {
        try {
            workbook = WorkbookFactory.create(stream);
        } catch (IOException e) {
            throw Exceptions.propagate(e);
        }
        this.describer = new GeneralBeanExcelDescriber<>(findGenericTypeFromSuperClass(getClass()));
    }

    @Override
    public Batch<T> readInBatch(int batchSize) throws EtlException {
        val sheetInfo = describer.getWorkSheetInfo();
        Sheet workingSheet;
        if(Strings.isNullOrEmpty(sheetInfo.getSheetName()))
            workingSheet = workbook.getSheetAt(sheetInfo.getSheetIndex());
        else
            workingSheet = workbook.getSheet(sheetInfo.getSheetName());

        int headerRowCount = sheetInfo.getHeaderEndRow() - sheetInfo.getHeaderStartRow();

        Iterator<Row> iterator = workingSheet.rowIterator();
        while (headerRowCount-- > 0) processingHeaderRow(iterator.next());

        return new ExcelBatchObject<>(workingSheet, iterator, batchSize, describer);
    }

    protected void processingHeaderRow(Row row) {


    }

    @Data
    public static class ParentBean {

        @Column("E")
        private Long id;

    }

    @WorkSheet
    @Data
    @ToString(callSuper = true)
    @EqualsAndHashCode(callSuper = true)
    public static class ExcelBean extends ParentBean {

        @Column(value = "A", header = @HeaderInfo("名称"))
        private String name;

        @Column("C")
        private LocalDate time;

        @Column("B")
        private String text;

    }

    public static void main(String[] args) throws IOException, EtlException {

        InputStream stream = Files.newInputStream(
                Paths.get("C:\\Users\\WHRDD-PC104\\Downloads\\test.xlsx"), StandardOpenOption.READ);
        val sw = Stopwatch.createStarted();
        val input = new ExcelInput<ExcelBean>(stream) {};
        input.readCollection();
        System.out.println(sw.stop());

    }
}























