package com.limin.etltool.core;

import com.google.common.reflect.TypeResolver;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/16
 */
public class GeneralTest {

    @Test
    public void test() {

        Logger logger = LoggerFactory.getLogger(GeneralTest.class);
        logger.info("Hi");

    }

    static class A<T> {

        protected A() {

        }

    }

    @Test
    public void testTypeResolver() {

        TypeResolver resolver = new TypeResolver();

        A a = new A<GeneralTest>();

        System.out.println(resolver.resolveType(a.getClass()));
    }

}