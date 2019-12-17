package com.limin.etltool.database.util.nameconverter;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/17
 */
@Target({ ElementType.TYPE })
@Retention(RetentionPolicy.RUNTIME)
public @interface NameConverter {
    Class<? extends INameConverter> value() default NopNameConverter.class;
}
