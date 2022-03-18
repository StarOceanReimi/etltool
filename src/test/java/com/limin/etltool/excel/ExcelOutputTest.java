package com.limin.etltool.excel;

import com.limin.etltool.excel.annotation.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.val;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.util.CellRangeAddress;
import org.junit.Test;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.IntStream;

/**
 * <p>
 *
 * </p>
 *
 * @author 邱理 WHRDD-PC104
 * @since 2022/1/4
 */
public class ExcelOutputTest {

    public static class VerticalMergeCellValue implements ValueGenerator {

        @Override
        public Object value(Cell cell, Object data) {
            Sheet sheet = cell.getSheet();
            Cell previous = previousCell(cell);
            Optional<Tuple2<Integer, Cell>> firstCellWithIndex = findFirstCellInMergeRegion(previous, sheet);
            if (!firstCellWithIndex.isPresent() && sameCellValue(previous, data)) {
                CellRangeAddress addresses =
                        CellRangeAddress.valueOf(previous.getAddress() + ":" + cell.getAddress());
                sheet.addMergedRegion(addresses);
                return BLANK_CELL;
            }
            if (!firstCellWithIndex.isPresent()) return data;
            final Tuple2<Integer, Cell> firstCell = firstCellWithIndex.get();
            if (!sameCellValue(firstCell.getT2(), data))
                return data;
            sheet.removeMergedRegion(firstCell.getT1());
            CellRangeAddress addresses =
                    CellRangeAddress.valueOf(firstCell.getT2().getAddress() + ":" + cell.getAddress());
            sheet.addMergedRegion(addresses);
            return BLANK_CELL;
        }

        private Optional<Tuple2<Integer, Cell>> findFirstCellInMergeRegion(Cell previous, Sheet sheet) {
            return IntStream.range(0, sheet.getNumMergedRegions())
                    .mapToObj(i -> Tuples.of(i, sheet.getMergedRegion(i)))
                    .filter(t -> t.getT2().isInRange(previous))
                    .map(t -> Tuples.of(t.getT1(), sheet.getRow(t.getT2().getFirstRow()).getCell(t.getT2().getFirstColumn())))
                    .findFirst();
        }

        private boolean sameCellValue(Cell cell, Object data) {
            return Objects.equals(cell.getStringCellValue(), data);
        }

        private Cell previousCell(Cell cell) {
            return cell.getSheet().getRow(cell.getRowIndex() - 1).getCell(cell.getColumnIndex());
        }
    }

    @WorkSheet
    @AllArgsConstructor
    @NoArgsConstructor
    @Data
    public static class TestBean {

        @Column(value = "A",
                header = @HeaderInfo("hello"),
                cellValue = @Value(generator = VerticalMergeCellValue.class))
        private String a;

    }


    @Test
    public void test0() throws Exception {

        final OutputStream outputStream = Files.newOutputStream(Paths.get("d:/testtest.xlsx"));
        val out = new ExcelOutput<TestBean>(outputStream) {};
        final List<TestBean> testBeans = Arrays.asList(
                new TestBean("a"),
                new TestBean("a"),
                new TestBean("a"),
                new TestBean("b"),
                new TestBean("b"),
                new TestBean("d"),
                new TestBean("c")
        );
        out.writeCollection(testBeans);
        out.close();

    }

}