package com.limin.etltool.core;

import java.util.Collection;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/18
 */
public interface Reducer<E, C extends Collection<E>, O> extends Transformer<C, O> {

    O accumulate(O out, E data);

    O init();

    @Override
    default O transform(C data) {
        O result = init();
        for(E datum : data)
            result = accumulate(result, datum);
        return result;
    };
}
