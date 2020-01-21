package com.limin.etltool.excel;

import com.google.common.base.Strings;
import com.limin.etltool.core.EtlException;
import com.limin.etltool.core.Output;
import com.limin.etltool.util.Exceptions;
import lombok.val;
import org.apache.commons.collections.CollectionUtils;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static com.limin.etltool.util.ReflectionUtils.findGenericTypeFromSuperClass;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;

/**
 * <p>
 *
 * </p>
 *
 * @author 邱理 WHRDD-PC104
 * @since 2020/1/21
 */
public class ExcelOutput<T> implements Output<T>, AutoCloseable {

    private final OutputStream outputStream;

    private final Workbook workbook;

    private final GeneralBeanExcelDescriber<T> describer;

    protected ExcelOutput(OutputStream outputStream) {
        this.outputStream = outputStream;
        try {
            workbook = WorkbookFactory.create(true);
        } catch (IOException e) {
            throw Exceptions.propagate(e);
        }
        describer = new GeneralBeanExcelDescriber<>(findGenericTypeFromSuperClass(getClass()));
    }

    @Override
    public boolean writeCollection(Collection<T> dataCollection) throws EtlException {

        if(CollectionUtils.isEmpty(dataCollection)) return false;

        val sheetInfo = describer.getWorkSheetInfo();

        Sheet workingSheet;
        if(Strings.isNullOrEmpty(sheetInfo.getSheetName()))
            workingSheet = workbook.createSheet();
        else
            workingSheet = workbook.createSheet(sheetInfo.getSheetName());

        int headerLen = sheetInfo.getHeaderEndRow() - sheetInfo.getHeaderStartRow();
        int i, len;
        for(i = sheetInfo.getHeaderStartRow(), len = i + headerLen; i < len; i++) {
            Row row = workingSheet.createRow(i);
            describer.writeHeader(row);
        }

        for (T bean : dataCollection) {
            Row row = workingSheet.createRow(i++);
            describer.writeCell(row, bean);
        }

        try {
            workbook.write(outputStream);
            outputStream.flush();
        } catch (IOException e) {
            throw Exceptions.propagate(e);
        }

        return true;
    }

    @Override
    public void close() throws Exception {
        workbook.close();
    }

    public static void main(String[] args) throws IOException, EtlException {
        OutputStream stream = Files.newOutputStream(Paths
                .get("C:\\Users\\WHRDD-PC104\\Downloads\\test1.xlsx"), CREATE, TRUNCATE_EXISTING);
        val output = new ExcelOutput<ExcelInput.ExcelBean>(stream) {};
        val bean = new ExcelInput.ExcelBean();
        bean.setName("ASD");
        bean.setText("HAHA");
        bean.setTime(LocalDate.now());
        bean.setId(123L);

        val bean1 = new ExcelInput.ExcelBean();
        bean1.setName("GDFSSDG");
        bean1.setText("HAHA!!!");
        bean1.setTime(LocalDate.of(2019, 12, 7));
        bean1.setId(122L);
        output.writeCollection(Arrays.asList(bean, bean1));
    }
}
