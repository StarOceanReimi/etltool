package com.limin.etltool.database.util.nameconverter;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/17
 */
public interface INameConverter {

    INameConverter DEFAULT = new NopNameConverter();

    static INameConverter getConverter(Class<?> converterClass) {

        if(!converterClass.isAnnotationPresent(NameConverter.class)) return DEFAULT;

        NameConverter converterAnno = converterClass.getAnnotation(NameConverter.class);
        try {
            return converterAnno.value().newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            return DEFAULT;
        }
    }

    static INameConverter getReverseConverter(Class<?> converterClass) {
        return getConverter(converterClass).getReverse();
    }

    String rename(String name);

    INameConverter getReverse();
}
