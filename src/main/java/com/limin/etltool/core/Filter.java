package com.limin.etltool.core;

import java.util.ArrayList;
import java.util.Collection;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/18
 */
public interface Filter<E, C extends Collection<E>> extends Transformer<C, C> {

    boolean filter(E element);

    @SuppressWarnings("unchecked")
    default C init() {
        return (C) new ArrayList<E>();
    }

    @Override
    default C transform(C data) {
        C result = init();
        for (E datum : data) {
            if (filter(datum)) result.add(datum);
        }
        return result;
    }
}
