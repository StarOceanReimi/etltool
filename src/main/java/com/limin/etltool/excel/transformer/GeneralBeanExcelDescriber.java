package com.limin.etltool.excel.transformer;

import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import com.limin.etltool.excel.annotation.Column;
import com.limin.etltool.excel.annotation.Sheet;
import com.limin.etltool.util.Exceptions;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.commons.beanutils.ConstructorUtils;
import org.apache.commons.beanutils.ConvertUtils;
import org.apache.poi.ss.usermodel.*;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;

import static com.limin.etltool.excel.util.IndexUtils.indexStringValue;

/**
 * <p>
 *
 * </p>
 *
 * @author 邱理 WHRDD-PC104
 * @since 2020/1/20
 */
class GeneralBeanExcelDescriber implements Iterable<GeneralBeanExcelDescriber.Meta> {

    @Override
    public Iterator<Meta> iterator() {
        return metaSet.iterator();
    }

    @EqualsAndHashCode
    class Meta {

        private int column;

        private Field field;

        public Meta(int column, Field field) {
            this.column = column;
            this.field = field;
        }

        public int getColumn() {
            return column;
        }

        public void consumeRow(Row row) {
            Cell cell = row.getCell(column);
            try {
                Class<?> type = field.getType();
                Object value = null;
                if(cell.getCellType() == CellType.NUMERIC) {
                    value = ConvertUtils.convert(cell.getNumericCellValue(), type);
                } else if(cell.getCellType() == CellType.STRING) {
                    value = ConvertUtils.convert(cell.getStringCellValue(), type);
                }
                field.setAccessible(true);
                field.set(GeneralBeanExcelDescriber.this.targetInstance, value);
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }

        }
    }

    private final Set<Meta> metaSet;

    private Object targetInstance;

    GeneralBeanExcelDescriber(Class<?> clazz) {
        Objects.requireNonNull(clazz);
        metaSet = Sets.newHashSet();
        Constructor<?> constructor =
                ConstructorUtils.getAccessibleConstructor(clazz, new Class<?>[0]);
        if(constructor == null)
            throw Exceptions.inform("Class {} must have a no args constructor", clazz.getSimpleName());
        try {
            targetInstance = constructor.newInstance();
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw Exceptions.inform("Cannot invoke ctor {}.", constructor);
        }
        traverseClassToFindFields(clazz);
    }

    private void traverseClassToFindFields(Class<?> clazz) {
        if(Object.class.equals(clazz)) return;
        Arrays.stream(clazz.getDeclaredFields())
                .filter(field -> field.isAnnotationPresent(Column.class))
                .map(this::toMeta)
                .forEach(metaSet::add);
        traverseClassToFindFields(clazz.getSuperclass());
    }

    private Meta toMeta(Field field) {
        Column column = field.getAnnotation(Column.class);
        return new Meta(indexStringValue(column.value()) - 1, field);
    }

    Object instance() {
        return targetInstance;
    }

    @Data
    public static class ParentBean {

        @Column("E")
        private Long id;

    }

    @Sheet
    @Data
    @EqualsAndHashCode(callSuper = true)
    public static class ExcelBean extends ParentBean {

        @Column("A")
        private String name;

        @Column("B")
        private String title;

        @Column("C")
        private String text;

    }

    public static void main(String[] args) throws IOException {

        Workbook workbook = WorkbookFactory.create(
                new FileInputStream("C:\\Users\\WHRDD-PC104\\Downloads\\test.xlsx"));

        Row row = workbook.getSheetAt(0).getRow(0);

        System.out.println(new GeneralBeanTransformer(ExcelBean.class).transform(row));
    }
}
