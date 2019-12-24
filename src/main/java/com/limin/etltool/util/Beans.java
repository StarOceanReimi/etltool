package com.limin.etltool.util;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Maps;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.val;
import net.sf.cglib.beans.BeanCopier;
import net.sf.cglib.core.Converter;
import org.apache.commons.beanutils.PropertyUtils;

import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static com.limin.etltool.util.Exceptions.propagate;
import static com.limin.etltool.util.Exceptions.rethrow;

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

        private static final Object[] EMPTY_ARGS = new Object[0];

        private final Class<?> clazz;
        private final Map<String, Method> propertiesReadCache;
        private final Map<String, Method> propertiesWriteCache;

        private FastBeanOperation(Class<?> clazz) {
            this.clazz = clazz;
            propertiesReadCache = Maps.newHashMap();
            propertiesWriteCache = Maps.newHashMap();
            loadAllProperties(clazz);
        }

        private void loadAllProperties(Class<?> clazz) {
            PropertyDescriptor[] descriptors = PropertyUtils.getPropertyDescriptors(clazz);
            for (PropertyDescriptor descriptor : descriptors) {
                if(descriptor.getDisplayName().equals("class")) continue;
                propertiesReadCache.put(descriptor.getDisplayName(), descriptor.getReadMethod());
                propertiesWriteCache.put(descriptor.getDisplayName(), descriptor.getReadMethod());
            }
        }

        public Set<String> getReaderNames() {
            return propertiesReadCache.keySet();
        }

        public Set<String> getWriterNames() {
            return propertiesWriteCache.keySet();
        }

        public Map<String, Object> describeAsMap(final Object bean) {
            return describeAsMap(bean, HashMap::new);
        }

        public Map<String, Object> describeAsMap(final Object bean, Supplier<Map<String, Object>> init) {
            Map<String, Object> result = init.get();
            getReaderNames().forEach(key -> {
                result.put(key, invokeGetter(bean, key));
            });
            return result;
        }

        public Object populateBean(Map<String, Object> map) {
            Object bean;
            try {
                bean = clazz.newInstance();
            } catch (InstantiationException | IllegalAccessException e) {
                throw propagate(e);
            }

            getWriterNames().forEach(key -> {
                invokeSetter(bean, key, map.get(key));
            });

            return bean;
        }


        public Object invokeGetter(Object bean, String name) {
            Method method = propertiesReadCache.get(name);
            if(method == null)
                throw Exceptions.inform("cannot find such property: {} in class {}", name, bean.getClass());
            try {
                return method.invoke(bean, EMPTY_ARGS);
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw propagate(e);
            }
        }

        public void invokeSetter(Object bean, String name, Object value) {
            Method method = propertiesWriteCache.get(name);
            if(method == null)
                throw Exceptions.inform("cannot find such property: {} in class {}", name, bean.getClass());
            try {
                method.invoke(bean, value);
            } catch (IllegalAccessException | InvocationTargetException e) {
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

        int N = 1_000_000;

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
        Map<Class, FastBeanOperation> cache = Maps.newHashMap();
        cache.computeIfAbsent(bean1.getClass(), Beans::getFastBeanOperation);
        FastBeanOperation beanOperation = cache.get(bean1.getClass());
        sw.start();
        for (int i=0; i<N; i++) {

            for (String key : beanOperation.getReaderNames()) {
                beanOperation.invokeGetter(bean1, key);
            }

//            PropertyDescriptor[] descriptors = PropertyUtils.getPropertyDescriptors(bean1.getClass());
//            for(PropertyDescriptor descriptor : descriptors) {
//                if(descriptor.getDisplayName().equals("class")) continue;
//                    method.invoke(bean1);
//                try {
//            beanOperation.invokeGetter(bean1, "id");
//                } catch (ExecutionException e) {
//                    e.printStackTrace();
//                }
//            }
        }
        System.out.println("FAST INVOKE");
        System.out.println(sw.elapsed(TimeUnit.MILLISECONDS));
        sw.reset();
        sw.start();
        for (int i=0; i<N; i++) {
            bean1.getId();
            bean1.getName();
        }
        System.out.println("DIRECT INVOKE");
        System.out.println(sw.elapsed(TimeUnit.MILLISECONDS));
    }

    public static void main(String[] args) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {

        fastMethodPerformanceTest();

    }

}
