package com.limin.etltool.excel;

import com.google.common.base.Strings;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.limin.etltool.excel.annotation.*;
import com.limin.etltool.util.Exceptions;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.beanutils.ConstructorUtils;
import org.apache.commons.beanutils.ConvertUtils;
import org.apache.commons.collections4.CollectionUtils;
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

    private static final String LONG_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
    private static final String SHORT_DATE_FORMAT = "yyyy-MM-dd";

    private final Set<ClassDataMeta> metaSet;

    private final Constructor<T> instanceCtor;

    private final WorkSheetInfo workSheetInfo;

    private final Cache<Class<?>, Object> classCache = CacheBuilder.newBuilder().build();

    private final Object outputContext;

    GeneralBeanExcelDescriber(Class<T> clazz, Object outputContext) {
        Objects.requireNonNull(clazz);
        instanceCtor = ConstructorUtils.getAccessibleConstructor(clazz, new Class<?>[0]);
        if (instanceCtor == null)
            throw Exceptions.inform("Class {} must have a no args constructor", clazz.getSimpleName());
        this.outputContext = outputContext;
        workSheetInfo = new WorkSheetInfo();
        workSheetInfo.from(clazz.getAnnotation(WorkSheet.class));
        metaSet = Sets.newTreeSet();
        traverseClassToFindFields(clazz);
    }

    WorkSheetInfo getWorkSheetInfo() {
        return workSheetInfo;
    }

    private void traverseClassToFindFields(Class<?> clazz) {
        if (Object.class.equals(clazz)) return;
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
        private int headerEndRow = 0;
        private String sheetName = "";
        private int sheetIndex = 0;
        private final Set<CellStyleSetter> headerDefaultSetter = Sets.newHashSet();
        private final Set<CellStyleSetter> valueDefaultSetter = Sets.newHashSet();

        private void from(WorkSheet sheetInfo) {
            if (sheetInfo == null) return;
            int[] headerRange = sheetInfo.headerRange();
            headerStartRow = headerRange[0];
            headerEndRow = headerRange[1];
            sheetName = sheetInfo.indexName();
            sheetIndex = sheetInfo.value();
            newStyleSetters(headerDefaultSetter, sheetInfo.headerDefaultStyle());
            newStyleSetters(valueDefaultSetter, sheetInfo.valueDefaultStyle());
        }

    }

    private void newStyleSetters(Set<CellStyleSetter> container, Class<? extends CellStyleSetter>[] styleSetter) {
        for (Class<? extends CellStyleSetter> setterClass : styleSetter) {
            Object setter = classCache.getIfPresent(setterClass);
            if (setter == null) {
                setter = newInstance(setterClass);
                if (setter != null)
                    classCache.put(setterClass, setter);
            }
            if (setter != null) {
                container.add((CellStyleSetter) setter);
            }
        }
    }

    private Object newInstance(Class<?> clazz) {
        try {
            return clazz.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            return null;
        }
    }

    @EqualsAndHashCode
    class ClassDataMeta implements Comparable<ClassDataMeta> {

        private int columnIdx;

        private Field field;

        private Map<CellAddress, Object> headerMemo;

        private Map<CellAddress, Set<CellStyleSetter>> headerStyleMemo;

        private Set<CellRangeAddress> tobeMerged;

        private Set<CellStyleSetter> valueStyleSetter;

        private ValueGenerator valueGenerator;

        private String constantValue;

        private String dataFormat;

        ClassDataMeta(Field field, Column column) {
            this.columnIdx = indexStringValue(column.value()) - 1;
            this.field = field;
            this.headerMemo = Maps.newHashMap();
            this.headerStyleMemo = Maps.newHashMap();
            this.tobeMerged = Sets.newHashSet();
            Value value = column.cellValue();
            constantValue = value.constant();
            valueGenerator = value.generator().isInterface() ? null : (ValueGenerator) newInstance(value.generator());
            if (valueGenerator != null)
                valueGenerator.setContext(outputContext);
            valueStyleSetter = Sets.newLinkedHashSet();
            valueStyleSetter.addAll(workSheetInfo.valueDefaultSetter);
            newStyleSetters(valueStyleSetter, column.valueCellStyle());
            dataFormat = column.dataFormat();
            parseHeaderInfo(column.header());
        }

        void parseHeaderInfo(HeaderInfo[] headerInfo) {

            if (headerInfo == null || headerInfo.length < 1) {
                CellAddress cellAddress =
                        new CellAddress(workSheetInfo.headerStartRow, columnIdx);
                headerMemo.put(cellAddress, field.getName());
                return;
            }

            for (HeaderInfo info : headerInfo) {
                CellRangeAddress address = CellRangeAddress.valueOf(info.address());
                CellAddress realAddress;
                if (address.isFullColumnRange()) {
                    realAddress = new CellAddress(workSheetInfo.headerStartRow, columnIdx);
                } else if (address.getNumberOfCells() > 1) {
                    realAddress = new CellAddress(address.getFirstRow(), address.getFirstColumn());
                    tobeMerged.add(address);
                } else {
                    realAddress = new CellAddress(address.getFirstRow(), address.getFirstColumn());
                }
                Object value = info.value();
                if (Strings.isNullOrEmpty(String.valueOf(value))) {
                    Value v = info.dynamicValue();
                    value = v.constant();
                    if (Strings.isNullOrEmpty(String.valueOf(value)) && !v.generator().isInterface()) {
                        ValueGenerator gen = (ValueGenerator) newInstance(v.generator());
                        if (gen != null) {
                            gen.setContext(outputContext);
                            value = gen;
                        }
                    }
                }
                headerMemo.put(realAddress, value);
                if (!headerStyleMemo.containsKey(realAddress)) {
                    Set<CellStyleSetter> set = Sets.newLinkedHashSet();
                    set.addAll(workSheetInfo.headerDefaultSetter);
                    newStyleSetters(set, info.headerCellStyle());
                    headerStyleMemo.put(realAddress, set);
                }
            }
        }

        void writeHeader(Row headerRow) {
            //add merged area
            tobeMerged.forEach(addr -> {
                if (!headerRow.getSheet().getMergedRegions().contains(addr)) {
                    headerRow.getSheet().addMergedRegion(addr);
                }
            });
            Cell cell = headerRow.createCell(columnIdx);
            if (headerMemo.containsKey(cell.getAddress())) {
                Object value = headerMemo.get(cell.getAddress());
                Set<CellStyleSetter> headerStyleSetter = headerStyleMemo.get(cell.getAddress());
                if (CollectionUtils.isNotEmpty(headerStyleSetter)) {
                    CellStyle style = createStyle(cell);
                    headerStyleSetter.forEach(setter -> setter.applyStyle(cell, style));
                    cell.setCellStyle(style);
                }
                if (value instanceof String) {
                    cell.setCellValue((String) value);
                } else if (value instanceof ValueGenerator) {
                    Object retValue = ((ValueGenerator) value).value(cell, null);
                    if (retValue == ValueGenerator.BLANK_CELL)
                        throw Exceptions.inform("Header Cannot to be blank!");
                    cell.setCellValue(String.valueOf(retValue));
                }
            }
        }

        private CellStyle createStyle(Cell cell) {
            Workbook wb = cell.getSheet().getWorkbook();
            return wb.createCellStyle();
        }


        void writeValue(Row row, T bean) {
            try {
                field.setAccessible(true);
                String df = Strings.isNullOrEmpty(dataFormat) ? null : dataFormat;
                Cell cell = row.createCell(columnIdx);
                Object defaultValue = null;
                Object value = null;
                if (!Strings.isNullOrEmpty(constantValue)) {
                    defaultValue = constantValue;
                } else if (valueGenerator != null) {
                    value = field.get(bean);
                    defaultValue = valueGenerator.value(cell, value);
                }
                if (defaultValue != null) {
                    value = defaultValue;
                } else if (value == null) {
                    value = field.get(bean);
                }
                if (value instanceof Number) {
                    cell.setCellValue(((Number) value).doubleValue());
                } else if (value instanceof CharSequence) {
                    cell.setCellValue((String) value);
                } else if (value instanceof Boolean) {
                    cell.setCellValue((Boolean) value);
                } else if (value instanceof Date) {
                    df = ofNullable(df).orElse(LONG_DATE_FORMAT);
                    cell.setCellValue((Date) value);
                } else if (value instanceof Calendar) {
                    df = ofNullable(df).orElse(LONG_DATE_FORMAT);
                    cell.setCellValue((Calendar) value);
                } else if (value instanceof LocalDateTime) {
                    df = ofNullable(df).orElse(LONG_DATE_FORMAT);
                    cell.setCellValue((LocalDateTime) value);
                } else if (value instanceof LocalDate) {
                    df = ofNullable(df).orElse(SHORT_DATE_FORMAT);
                    cell.setCellValue(((LocalDate) value).atStartOfDay());
                } else {
                    if (value != ValueGenerator.BLANK_CELL) {
                        ofNullable(value).map(Object::toString).ifPresent(cell::setCellValue);
                    }
                }
                if (!valueStyleSetter.isEmpty() || defaultValue != null) {
                    CellStyle style = createStyle(cell);
                    if (!valueStyleSetter.isEmpty())
                        valueStyleSetter.forEach(setter -> setter.applyStyle(cell, style));
                    if (df != null) {
                        DataFormat format = cell.getSheet().getWorkbook().createDataFormat();
                        style.setDataFormat(format.getFormat(df));
                    }
                    cell.setCellStyle(style);
                }
            } catch (IllegalAccessException e) {
                log.warn("cannot access field: {}", field.getName());
            }
        }

        void setValue(Row row, Map<Cell, Cell> mergeContext, Object instance) {

            Cell cell = Objects.requireNonNull(row).getCell(columnIdx);
            if (cell == null) return;
            Cell realCell = ofNullable(mergeContext.get(cell)).orElse(cell);
            try {
                Class<?> type = field.getType();
                Object value = null;
                if (realCell.getCellType() == CellType.NUMERIC) {
                    if (LocalDate.class.isAssignableFrom(type)) {
                        value = realCell.getLocalDateTimeCellValue().toLocalDate();
                    } else if (LocalDateTime.class.isAssignableFrom(type)) {
                        value = realCell.getLocalDateTimeCellValue();
                    } else {
                        value = ConvertUtils.convert(realCell.getNumericCellValue(), type);
                    }
                } else if (realCell.getCellType() == CellType.STRING) {
                    value = ConvertUtils.convert(realCell.getStringCellValue(), type);
                } else if (realCell.getCellType() == CellType.BOOLEAN) {
                    value = ConvertUtils.convert(realCell.getBooleanCellValue(), type);
                }
                field.setAccessible(true);
                field.set(instance, value);
            } catch (IllegalAccessException e) {
                log.warn("cannot write field: {}", field.getName());
            }
        }

        @Override
        public int compareTo(ClassDataMeta o) {
            return Integer.compare(columnIdx, o.columnIdx);
        }
    }


}
