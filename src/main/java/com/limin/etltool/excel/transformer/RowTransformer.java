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
public interface RowTransformer<T> {

    T transform(Row row);
}
