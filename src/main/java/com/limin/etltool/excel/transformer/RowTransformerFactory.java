package com.limin.etltool.excel.transformer;

/**
 * <p>
 *
 * </p>
 *
 * @author 邱理 WHRDD-PC104
 * @since 2020/1/20
 */
public abstract class RowTransformerFactory {

    public static <T> RowTransformer<T> getTransformer(Class<? extends T> targetClass) {

        return null;
    }
}
