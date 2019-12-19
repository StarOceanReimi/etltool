package com.limin.etltool.core;

import com.google.common.base.Stopwatch;
import com.google.common.reflect.TypeResolver;
import lombok.val;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedList;

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

    @Test
    public void testListAdd() {

        val arrayList = new ArrayList<Integer>();
        val linkedList = new LinkedList<Integer>();
        val sw = Stopwatch.createStarted();
        int N = 800_0000;
        for(int i = 0; i < N; i++) arrayList.add(i);
        System.out.println(sw.elapsed().toMillis());
        sw.reset();
        sw.start();
        for(int i = 0; i < N; i++) linkedList.add(i);
        System.out.println(sw.elapsed().toMillis());

    }

}