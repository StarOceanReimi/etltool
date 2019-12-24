package com.limin.etltool.step;

import com.google.common.collect.Maps;
import com.limin.etltool.core.Transformer;
import com.limin.etltool.util.Beans;

import java.util.Map;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/21
 */
public abstract class CachedBeanOperationTransform<I, O> implements Transformer<I, O> {

    private final Map<Class<?>, Beans.FastBeanOperation> operationCache = Maps.newHashMap();

    protected Beans.FastBeanOperation loadOperation(Object data) {
        return operationCache.computeIfAbsent(data.getClass(), Beans::getFastBeanOperation);
    }
}
