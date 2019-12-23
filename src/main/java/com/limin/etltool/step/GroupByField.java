package com.limin.etltool.step;

import com.google.common.collect.Lists;
import com.limin.etltool.core.Reducer;

import java.util.Collection;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/21
 */
public class GroupBy<E, T, O extends Collection<T>> implements Reducer<E, Collection<E>, O> {

    @Override
    public O accumulate(O out, E data) {


        return null;
    }

    @Override
    public O init() {
        return (O) Lists.newArrayList();
    }
}
