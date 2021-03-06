package com.limin.etltool.excel.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * <p>
 *
 * </p>
 *
 * @author 邱理 WHRDD-PC104
 * @since 2020/1/20
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD})
public @interface Column {

    /**
     * 字段对应的列名如: A B C等
     */
    String value();

    HeaderInfo[] header() default {};

    Value cellValue() default @Value;

    String dataFormat() default "";

    Class<? extends CellStyleSetter>[] valueCellStyle() default {};
}
