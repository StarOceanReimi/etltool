package com.limin.etltool.excel;

import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.limin.etltool.core.Batch;
import com.limin.etltool.core.BatchInput;
import com.limin.etltool.core.EtlException;
import com.limin.etltool.database.DatabaseAccessor;
import com.limin.etltool.database.DatabaseConfiguration;
import com.limin.etltool.database.NormalDbOutput;
import com.limin.etltool.database.TableColumnAccessor;
import com.limin.etltool.database.mysql.DefaultMySqlDatabase;
import com.limin.etltool.excel.annotation.*;
import com.limin.etltool.excel.util.CellStyles;
import com.limin.etltool.excel.util.MergeCellValue;
import com.limin.etltool.excel.util.SerialNoGenerator;
import com.limin.etltool.util.Exceptions;
import com.limin.etltool.util.JavaTimeConverters;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.val;
import org.apache.commons.beanutils.ConvertUtils;
import org.apache.poi.openxml4j.exceptions.OpenXML4JException;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.ss.util.CellAddress;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Iterator;

import static com.limin.etltool.util.ReflectionUtils.findGenericTypeFromSuperClass;
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

        @Column(value = "E", header = @HeaderInfo(value = "PID", address = "E1:E2"))
        private Long pid;

    }

    @WorkSheet(
        headerDefaultStyle = {
            CellStyles.CenterAlignmentStyle.class,
            CellStyles.FontBoldStyle.class
        },
        headerRange = {0, 2}
    )
    @Data
    @ToString(callSuper = true)
    @EqualsAndHashCode(callSuper = true)
    public static class ExcelBean extends ParentBean {

        @Column(value = "F",
                header = @HeaderInfo(value = "序号", address = "F1:F2"),
                dataFormat = "0.00",
                cellValue = @Value(generator = SerialNoGenerator.class))
        private Integer serialNo;

        @Column(value = "A",
                header = @HeaderInfo(value = "名称", address = "A1:A2" ))
        private String name;

        @Column(value = "B",
                header = @HeaderInfo(
                    value = "TIME",
                    address = "B1:B2",
                    headerCellStyle = {
                        CellStyles.WiderLengthStyle.class
                    }
                ))
        private LocalDate time;

        @Column(value = "C",
                valueCellStyle = CellStyles.CenterAlignmentStyle.class,
                header = {
                    @HeaderInfo(value = "文本", address = "C1:D1"),
                    @HeaderInfo(value = "文本1", address = "C2")
                }
        )
        private String text;

        @Column(value = "D",
                cellValue = @Value(generator = MergeCellValue.class),
                header =  @HeaderInfo(value = "文本D", address = "D2")
        )
        private String testD;

    }

    public static void main(String[] args) throws IOException, EtlException, ParserConfigurationException, OpenXML4JException {

        InputStream stream = Files.newInputStream(Paths.get("c:\\users\\reimidesktop\\test.xlsx"), READ);
        val sw = Stopwatch.createStarted();
        val input = new ExcelInput<ExcelBean>(stream) {};

        DatabaseConfiguration configuration = new DatabaseConfiguration();
        DatabaseAccessor accessor = new TableColumnAccessor(TableColumnAccessor.SqlType.INSERT, "test_bean");
        val output = new NormalDbOutput<ExcelBean>(new DefaultMySqlDatabase(configuration), accessor) {};
        output.setCreateTableIfNotExists(true);
        output.writeCollection(input.readCollection());

        System.out.println(sw.stop());
//        sw.reset();
//        sw.start();

//        OPCPackage opcPackage = OPCPackage.open(stream);
//        XSSFReader reader = new XSSFReader(opcPackage);
//        ExcelContentHandler handler = new ExcelContentHandler(reader, null);
//        val sheetIter = reader.getSheetsData();
//        System.out.println(sw.stop());
//        try {
//            XMLReader xmlReader = SAXHelper.newXMLReader();
//            xmlReader.setContentHandler(handler);
//            sw.reset();
//            sw.start();
//            xmlReader.parse(new InputSource(sheetIter.next()));
//            System.out.println(sw.stop());
//        } catch (SAXException e) {
//            e.printStackTrace();
//        }

    }
}























