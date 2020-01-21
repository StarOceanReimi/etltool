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
import javafx.scene.input.DataFormat;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.val;
import org.apache.commons.beanutils.ConvertUtils;
import org.apache.poi.hssf.eventusermodel.HSSFRequest;
import org.apache.poi.ooxml.util.SAXHelper;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.openxml4j.exceptions.OpenXML4JException;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.ss.util.CellAddress;
import org.apache.poi.xssf.eventusermodel.XSSFBReader;
import org.apache.poi.xssf.eventusermodel.XSSFReader;
import org.apache.poi.xssf.eventusermodel.XSSFSheetXMLHandler;
import org.apache.poi.xssf.model.Comments;
import org.apache.poi.xssf.model.CommentsTable;
import org.apache.poi.xssf.model.StylesTable;
import org.apache.poi.xssf.usermodel.*;
import org.xml.sax.*;

import javax.sql.rowset.spi.XmlReader;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import static com.limin.etltool.util.ReflectionUtils.findGenericTypeFromSuperClass;
import static com.limin.etltool.util.ReflectionUtils.findPropertyNameWithAnnotation;
import static java.nio.file.StandardOpenOption.READ;

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

        @Column("B")
        private LocalDate time;

        @Column("C")
        private String text;

    }

    public static void main(String[] args) throws IOException, EtlException, ParserConfigurationException, OpenXML4JException {

        InputStream stream = Files.newInputStream(Paths.get("c:\\users\\reimidesktop\\test.xlsx"), READ);
        val sw = Stopwatch.createStarted();
//        val input = new ExcelInput<ExcelBean>(stream) {};
//        System.out.println(sw.stop());
//        sw.reset();
//        sw.start();
//        System.out.println(input.readCollection().size());
//        System.out.println(sw.stop());
//        sw.reset();
//        sw.start();

        OPCPackage opcPackage = OPCPackage.open(stream);
        XSSFReader reader = new XSSFReader(opcPackage);
        ExcelContentHandler handler = new ExcelContentHandler(reader, null);
        val sheetIter = reader.getSheetsData();
        System.out.println(sw.stop());
        try {
            XMLReader xmlReader = SAXHelper.newXMLReader();
            xmlReader.setContentHandler(handler);
            sw.reset();
            sw.start();
            xmlReader.parse(new InputSource(sheetIter.next()));
            System.out.println(sw.stop());
        } catch (SAXException e) {
            e.printStackTrace();
        }

    }
}























