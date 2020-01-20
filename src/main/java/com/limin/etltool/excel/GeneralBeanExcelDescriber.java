package com.limin.etltool.excel;

import com.google.common.collect.Sets;
import com.limin.etltool.excel.annotation.Column;
import com.limin.etltool.excel.annotation.WorkSheet;
import com.limin.etltool.util.Exceptions;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.commons.beanutils.ConstructorUtils;
import org.apache.commons.beanutils.ConvertUtils;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Row;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

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
class GeneralBeanExcelDescriber<T> {

    private final Set<ClassDataMeta> metaSet;

    private final Constructor<T> instanceCtor;

    private final WorkSheetInfo workSheetInfo;

    GeneralBeanExcelDescriber(Class<T> clazz) {
        Objects.requireNonNull(clazz);
        instanceCtor = ConstructorUtils.getAccessibleConstructor(clazz, new Class<?>[0]);
        if(instanceCtor == null)
            throw Exceptions.inform("Class {} must have a no args constructor", clazz.getSimpleName());
        metaSet = Sets.newHashSet();
        traverseClassToFindFields(clazz);

        workSheetInfo = new WorkSheetInfo();
        workSheetInfo.from(clazz.getAnnotation(WorkSheet.class));
    }

    public WorkSheetInfo getWorkSheetInfo() {
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
        return new ClassDataMeta(indexStringValue(column.value()) - 1, field);
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

    @Data
    class WorkSheetInfo {

        private int headerStartRow = 0;
        private int headerEndRow   = 0;
        private int dataStartRow   = 0;
        private String sheetName   = "";
        private int sheetIndex     = 0;

        private void from(WorkSheet sheetInfo) {
            if(sheetInfo == null) return;
            int[] headerRange = sheetInfo.headerRange();
            headerStartRow = headerRange[0];
            headerEndRow   = headerRange[1];
            dataStartRow   = sheetInfo.dataAreaStartRow();
            sheetName      = sheetInfo.indexName();
            sheetIndex     = sheetInfo.value();
        }

    }

    @EqualsAndHashCode
    class ClassDataMeta {

        private int column;

        private Field field;

        ClassDataMeta(int column, Field field) {
            this.column = column;
            this.field = field;
        }

        void setValue(Row row, Map<Cell, Cell> mergeContext, Object instance) {

            Cell cell = Objects.requireNonNull(row).getCell(column);
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
                e.printStackTrace();
            }
        }
    }
}
