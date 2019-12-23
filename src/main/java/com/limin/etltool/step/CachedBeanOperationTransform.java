package com.limin.etltool.step;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.limin.etltool.core.Transformer;
import com.limin.etltool.util.Beans;
import com.limin.etltool.util.Exceptions;

import java.util.concurrent.ExecutionException;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/21
 */
public abstract class CachedBeanOperationTransform<I, O> implements Transformer<I, O> {

    private static final LoadingCache<Class<?>, Beans.FastBeanOperation> operationCache =
            CacheBuilder.newBuilder().build(CacheLoader.from(Beans::getFastBeanOperation));

    protected Beans.FastBeanOperation loadOperation(Object data) {
        try {
            return operationCache.get(data.getClass());
        } catch (ExecutionException e) {
            throw Exceptions.propagate(e);
        }
    }
}
