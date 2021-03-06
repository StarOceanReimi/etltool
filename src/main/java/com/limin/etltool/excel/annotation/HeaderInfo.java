package com.limin.etltool.excel.annotation;

/**
 * <p>
 *
 * </p>
 *
 * @author 邱理 WHRDD-PC104
 * @since 2020/1/21
 */
public @interface HeaderInfo {

    String address() default "";

    Class<? extends CellStyleSetter>[] headerCellStyle() default {};

    String value() default "";

    Value dynamicValue() default @Value;
}
