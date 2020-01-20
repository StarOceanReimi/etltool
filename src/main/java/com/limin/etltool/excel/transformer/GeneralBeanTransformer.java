package com.limin.etltool.excel.transformer;

import org.apache.poi.ss.usermodel.Row;

/**
 * <p>
 *
 * </p>
 *
 * @author 邱理 WHRDD-PC104
 * @since 2020/1/20
 */
public class GeneralBeanTransformer<T> implements RowTransformer<T> {

    private final Class<T> clazz;

    private final GeneralBeanExcelDescriber describer;

    public GeneralBeanTransformer(Class<T> clazz) {
        this.clazz = clazz;
        describer = new GeneralBeanExcelDescriber(clazz);
    }

    @SuppressWarnings("unchecked")
    @Override
    public T transform(Row row) {
        describer.forEach(meta -> meta.consumeRow(row));
        return (T) describer.instance();
    }
}
