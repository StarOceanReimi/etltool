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
@Target({ElementType.TYPE})
public @interface WorkSheet {

    /**
     * 工作簿的序号
     */
    int value() default 0;

    /**
     * 工作簿的名称
     */
    String indexName() default "";

    /**
     * 工作簿表头开始结束行数
     */
    int[] headerRange() default { 0, 1 };

    Class<? extends CellStyleSetter>[] headerDefaultStyle() default {};

    Class<? extends CellStyleSetter>[] valueDefaultStyle() default {};
}
