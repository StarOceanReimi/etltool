package com.limin.etltool.excel.annotation;

public @interface Value {

    String constant() default "";

    Class<? extends ValueGenerator> generator() default ValueGenerator.class;
}
