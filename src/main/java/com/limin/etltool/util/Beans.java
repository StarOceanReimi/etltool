package com.limin.etltool.util;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.cache.*;
import com.google.common.collect.Maps;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.val;
import net.sf.cglib.beans.BeanCopier;
import net.sf.cglib.core.Converter;
import net.sf.cglib.reflect.FastClass;
import net.sf.cglib.reflect.FastMethod;
import org.apache.commons.beanutils.PropertyUtils;

import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.limin.etltool.util.Exceptions.propagate;
import static com.limin.etltool.util.Exceptions.rethrow;
import static java.util.Optional.ofNullable;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/21
 */
public abstract class Beans {

    public static void copy(Object dest, Object source) {
        copy(dest, source, null);
    }
    public static void copy(Object dest, Object source, Converter converter) {
        Preconditions.checkNotNull(source, "copy source cannot be null");
        Preconditions.checkNotNull(dest, "copy destination cannot be null");
        BeanCopier copier = BeanCopier.create(source.getClass(), dest.getClass(), converter != null);
        copier.copy(source, dest, converter);
    }

    public static FastBeanOperation getFastBeanOperation(Class<?> clazz) {
        return new FastBeanOperation(clazz);
    }

    public static class FastBeanOperation {

        private final FastClass fastClass;

        private final LoadingCache<Method, FastMethod> methodCache;
        private final Cache<String, FastMethod> propertiesReadCache;
        private final Cache<String, FastMethod> propertiesWriteCache;

        public FastBeanOperation(Class<?> clazz) {
            this.fastClass = FastClass.create(clazz);
            methodCache = CacheBuilder.newBuilder()
                    .build(CacheLoader.from(fastClass::getMethod));
            propertiesReadCache = CacheBuilder.newBuilder().build();
            propertiesWriteCache = CacheBuilder.newBuilder().build();
            loadAllProperties(clazz);
        }

        private void loadAllProperties(Class<?> clazz) {
            PropertyDescriptor[] descriptors = PropertyUtils.getPropertyDescriptors(clazz);
            for (PropertyDescriptor descriptor : descriptors) {
                if(descriptor.getDisplayName().equals("class")) continue;
                propertiesReadCache.put(descriptor.getDisplayName(), fastClass.getMethod(descriptor.getReadMethod()));
                propertiesWriteCache.put(descriptor.getDisplayName(), fastClass.getMethod(descriptor.getReadMethod()));
            }
        }

        public Object invokeGetter(Object bean, String name) {
            FastMethod method = propertiesReadCache.getIfPresent(name);
            if(method == null) return null;
            try {
                return method.invoke(bean, new Object[0]);
            } catch (InvocationTargetException e) {
                throw propagate(e);
            }
        }

        public void invokeSetter(Object bean, String name, Object value) {
            FastMethod method = propertiesWriteCache.getIfPresent(name);
            if(method == null) return;
            try {
                method.invoke(bean, new Object[] { value });
            } catch (InvocationTargetException e) {
                rethrow(e);
            }
        }

    }

    @AllArgsConstructor
    @Data
    public static class TestBean1 {
        private Number id;
        private String name;
    }

    @AllArgsConstructor
    @Data
    public static class TestBean2 {
        private Long id;
        private String name;
        private String test;
    }

    private static void fastMethodPerformanceTest() throws InvocationTargetException, IllegalAccessException, NoSuchMethodException {

        int N = 1_000;

        TestBean1 bean1 = new TestBean1(1L, "QL");

        val sw = Stopwatch.createStarted();
        for(int i=0; i<N; i++) {
            PropertyDescriptor[] descriptors = PropertyUtils.getPropertyDescriptors(bean1.getClass());
            for(PropertyDescriptor descriptor : descriptors) {
                if(descriptor.getDisplayName().equals("class")) continue;
                PropertyUtils.getProperty(bean1, descriptor.getDisplayName());
            }
        }
        System.out.println("NORMAL INVOKE");
        System.out.println(sw.elapsed(TimeUnit.MILLISECONDS));
        sw.reset();
        sw.start();
//        Map<Class<?>, FastBeanOperation> fastBeanOperationMap = Maps.newHashMap();
//        fastBeanOperationMap.put(bean1.getClass(), getFastBeanOperation(bean1.getClass()));
        LoadingCache<Class<?>, FastBeanOperation> cache = CacheBuilder.newBuilder()
                .build(CacheLoader.from(Beans::getFastBeanOperation));
//        FastBeanOperation beanOperation = getFastBeanOperation(bean1.getClass());
        for (int i=0; i<N; i++) {
            PropertyDescriptor[] descriptors = PropertyUtils.getPropertyDescriptors(bean1.getClass());
            for(PropertyDescriptor descriptor : descriptors) {
                if(descriptor.getDisplayName().equals("class")) continue;
                try {
                    cache.get(bean1.getClass()).invokeGetter(bean1, descriptor.getDisplayName());
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
            }
        }
        System.out.println("FAST INVOKE");
        System.out.println(sw.elapsed(TimeUnit.MILLISECONDS));
        sw.reset();
        sw.start();
        for (int i=0; i<N; i++) {
            bean1.getId();
        }
        System.out.println("DIRECT INVOKE");
        System.out.println(sw.elapsed(TimeUnit.MILLISECONDS));
    }

    public static void main(String[] args) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {

        fastMethodPerformanceTest();

    }

}
