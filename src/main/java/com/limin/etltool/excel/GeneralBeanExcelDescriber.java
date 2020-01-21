package com.limin.etltool.excel;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.limin.etltool.excel.annotation.Column;
import com.limin.etltool.excel.annotation.HeaderInfo;
import com.limin.etltool.excel.annotation.WorkSheet;
import com.limin.etltool.util.Exceptions;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.beanutils.ConstructorUtils;
import org.apache.commons.beanutils.ConvertUtils;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.ss.util.CellAddress;
import org.apache.poi.ss.util.CellRangeAddress;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;

import static com.limin.etltool.excel.util.IndexUtils.indexStringValue;
import static java.util.Optional.ofNullable;

/**
 * <p>
 *
 * </p>
 *
 * @author 邱理 WHRDD-PC104
 * @since 2020/1/20
 */
@Slf4j
class GeneralBeanExcelDescriber<T> {

    private final Set<ClassDataMeta> metaSet;

    private final Constructor<T> instanceCtor;

    private final WorkSheetInfo workSheetInfo;

    GeneralBeanExcelDescriber(Class<T> clazz) {
        Objects.requireNonNull(clazz);
        instanceCtor = ConstructorUtils.getAccessibleConstructor(clazz, new Class<?>[0]);
        if(instanceCtor == null)
            throw Exceptions.inform("Class {} must have a no args constructor", clazz.getSimpleName());
        workSheetInfo = new WorkSheetInfo();
        workSheetInfo.from(clazz.getAnnotation(WorkSheet.class));
        metaSet = Sets.newHashSet();
        traverseClassToFindFields(clazz);
    }

    WorkSheetInfo getWorkSheetInfo() {
        return workSheetInfo;
    }

    private void traverseClassToFindFields(Class<?> clazz) {
        if(Object.class.equals(clazz)) return;
        Arrays.stream(clazz.getDeclaredFields())
                .filter(field -> field.isAnnotationPresent(Column.class))
                .map(this::toMeta)
                .forEach(metaSet::add);
        traverseClassToFindFields(clazz.getSuperclass());
    }

    private ClassDataMeta toMeta(Field field) {
        Column column = field.getAnnotation(Column.class);
        return new ClassDataMeta(field, column);
    }

    T newBean(Row row, Map<Cell, Cell> mergeContext) {

        T targetInstance;
        try {
            targetInstance = instanceCtor.newInstance();
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw Exceptions.inform("Cannot invoke ctor {}.", instanceCtor);
        }
        metaSet.forEach(meta -> meta.setValue(row, mergeContext, targetInstance));
        return targetInstance;
    }

    void writeHeader(Row row) {
        metaSet.forEach(meta -> meta.writeHeader(row));
    }

    void writeCell(Row row, T bean) {
        metaSet.forEach(meta -> meta.writeValue(row, bean));
    }

    @Data
    class WorkSheetInfo {

        private int headerStartRow = 0;
        private int headerEndRow   = 0;
        private String sheetName   = "";
        private int sheetIndex     = 0;

        private void from(WorkSheet sheetInfo) {
            if(sheetInfo == null) return;
            int[] headerRange = sheetInfo.headerRange();
            headerStartRow = headerRange[0];
            headerEndRow   = headerRange[1];
            sheetName      = sheetInfo.indexName();
            sheetIndex     = sheetInfo.value();
        }

    }

    @EqualsAndHashCode
    class ClassDataMeta {

        private int columnIdx;

        private Field field;

        private Map<CellAddress, String> headerMemo;

        private Set<CellRangeAddress> tobeMerged;

        ClassDataMeta(Field field, Column column) {
            this.columnIdx = indexStringValue(column.value()) - 1;
            this.field = field;
            this.headerMemo = Maps.newHashMap();
            this.tobeMerged = Sets.newHashSet();
            parseHeaderInfo(column.header());
        }

        void parseHeaderInfo(HeaderInfo[] headerInfo) {

            if(headerInfo == null || headerInfo.length < 1){
                CellAddress cellAddress =
                        new CellAddress(workSheetInfo.headerStartRow, columnIdx);
                headerMemo.put(cellAddress, field.getName());
                return;
            }

            for (HeaderInfo info : headerInfo) {
                CellRangeAddress address = CellRangeAddress.valueOf(info.address());
                if(address.isFullColumnRange() || address.getNumberOfCells() > 1) {
                    CellAddress cellAddress =
                            new CellAddress(workSheetInfo.headerStartRow, columnIdx);
                    headerMemo.put(cellAddress, info.value());
                } else {
                    CellAddress cellAddress =
                            new CellAddress(address.getFirstRow(), address.getFirstColumn());
                    headerMemo.put(cellAddress, info.value());
                    tobeMerged.add(address);
                }
            }
        }

        void writeHeader(Row headerRow) {
            //add merged area
            if(!tobeMerged.isEmpty()) {
                tobeMerged.forEach(headerRow.getSheet()::addMergedRegion);
                tobeMerged.clear();
            }
            Cell cell = headerRow.createCell(columnIdx);
            if(headerMemo.containsKey(cell.getAddress())) {
                String value = headerMemo.get(cell.getAddress());
                cell.setCellValue(value);
            }
        }

        private void createDateStyle(Cell cell, String dateFormat) {
            Workbook wb = cell.getSheet().getWorkbook();
            CellStyle style = wb.createCellStyle();
            DataFormat format = wb.createDataFormat();
            style.setDataFormat(format.getFormat(dateFormat));
            cell.setCellStyle(style);
        }

        void writeValue(Row row, T bean) {
            try {
                field.setAccessible(true);
                Object value = field.get(bean);
                if(value instanceof Number) {
                    Cell cell = row.createCell(columnIdx, CellType.NUMERIC);
                    cell.setCellValue(((Number) value).doubleValue());
                } else if(value instanceof CharSequence) {
                    Cell cell = row.createCell(columnIdx, CellType.STRING);
                    cell.setCellValue((String) value);
                } else if(value instanceof Boolean) {
                    Cell cell = row.createCell(columnIdx, CellType.BOOLEAN);
                    cell.setCellValue((Boolean) value);
                } else if(value instanceof Date) {
                    Cell cell = row.createCell(columnIdx, CellType.STRING);
                    createDateStyle(cell, "yyyy-MM-dd HH:mm:ss");
                    cell.setCellValue((Date) value);
                } else if(value instanceof Calendar) {
                    Cell cell = row.createCell(columnIdx, CellType.STRING);
                    createDateStyle(cell, "yyyy-MM-dd HH:mm:ss");
                    cell.setCellValue((Calendar) value);
                } else if(value instanceof LocalDateTime) {
                    Cell cell = row.createCell(columnIdx, CellType.STRING);
                    createDateStyle(cell, "yyyy-MM-dd HH:mm:ss");
                    cell.setCellValue((LocalDateTime) value);
                } else if(value instanceof LocalDate) {
                    Cell cell = row.createCell(columnIdx, CellType.STRING);
                    createDateStyle(cell, "yyyy-MM-dd");
                    cell.setCellValue(((LocalDate) value).atStartOfDay());
                } else {
                    Cell cell = row.createCell(columnIdx, CellType.STRING);
                    ofNullable(value).map(Object::toString).ifPresent(cell::setCellValue);
                }
            } catch (IllegalAccessException e) {
                log.warn("cannot access field: {}", field.getName());
            }
        }

        void setValue(Row row, Map<Cell, Cell> mergeContext, Object instance) {

            Cell cell = Objects.requireNonNull(row).getCell(columnIdx);
            Cell realCell = ofNullable(mergeContext.get(cell)).orElse(cell);
            try {
                Class<?> type = field.getType();
                Object value = null;
                if(realCell.getCellType() == CellType.NUMERIC) {
                    if(LocalDateTime.class.isAssignableFrom(type)) {
                        value = realCell.getLocalDateTimeCellValue();
                    } else {
                        value = ConvertUtils.convert(realCell.getNumericCellValue(), type);
                    }
                } else if(realCell.getCellType() == CellType.STRING) {
                    value = ConvertUtils.convert(realCell.getStringCellValue(), type);
                } else if(realCell.getCellType() == CellType.BOOLEAN) {
                    value = ConvertUtils.convert(realCell.getBooleanCellValue(), type);
                }
                field.setAccessible(true);
                field.set(instance, value);
            } catch (IllegalAccessException e) {
                log.warn("cannot write field: {}", field.getName());
            }
        }
    }
}
