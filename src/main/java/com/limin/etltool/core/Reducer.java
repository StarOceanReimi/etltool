package com.limin.etltool.core;

import java.util.Collection;
import java.util.stream.Stream;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/18
 */
public interface Reducer<E, C extends Stream<E>, O> extends Transformer<C, O> {

    O accumulate(O out, E data);

    O init();

    default O combine(O o1, O o2) {
        return o1;
    }

    @Override
    default O transform(C data) {
        return data.reduce(init(), this::accumulate, this::combine);
    };
}
