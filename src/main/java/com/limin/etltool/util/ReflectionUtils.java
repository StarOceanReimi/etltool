package com.limin.etltool.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.beanutils.PropertyUtils;

import java.beans.PropertyDescriptor;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/19
 */
@Slf4j
public abstract class ReflectionUtils {

    public static String findPropertyNameWithAnnotation(Object bean, Class<? extends Annotation> annotationClass) {
        Object o = findPropertyWithAnnotation(bean, annotationClass);
        if(o == null) return null;
        if(o instanceof Field) return ((Field) o).getName();
        if(o instanceof Method) return ((Method) o).getName();
        return null;
    }

    public static Object findPropertyValueWithAnnotation(Object bean, Class<? extends Annotation> annotationClass) {

        Object o = findPropertyWithAnnotation(bean, annotationClass);
        if(o == null) return null;
        if(o instanceof Field) {
            ((Field) o).setAccessible(true);
            try {
                return ((Field) o).get(bean);
            } catch (IllegalAccessException e) {
                log.warn("cannot get field value from bean: {}", bean);
                return null;
            }
        }

        if(o instanceof Method) {
            ((Method) o).setAccessible(true);
            try {
                return ((Method) o).invoke(bean);
            } catch (IllegalAccessException | InvocationTargetException e) {
                log.warn("cannot invoke method from bean: {}", bean);
                return null;
            }
        }

        return null;
    }

    private static Object findPropertyWithAnnotation(Object bean, Class<? extends Annotation> annotationClass) {
        PropertyDescriptor[] descriptors = PropertyUtils.getPropertyDescriptors(bean.getClass());
        for(PropertyDescriptor descriptor : descriptors) {
            Method read = descriptor.getReadMethod();
            if(read.isAnnotationPresent(annotationClass))
                return read;
        }
        Field[] fields = bean.getClass().getDeclaredFields();
        for (Field f : fields) {
            if(f.isAnnotationPresent(annotationClass))
                return f;
        }
        return null;
    }
}
