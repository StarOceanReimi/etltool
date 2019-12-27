package com.limin.etltool.core;

import java.util.Collection;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/16
 */
public interface Operation<I, O> {

    default void process(Input<I> input, Output<I> output) throws EtlException {
        Collection<I> inputCollection = input.readCollection();
        output.writeCollection(inputCollection);
    }

    default void process(Input<I> input,
                         Transformer<Collection<I>, Collection<O>> transformer,
                         Output<O> output) throws EtlException {
        Collection<I> inputCollection = input.readCollection();
        output.writeCollection(transformer.transform(inputCollection));
    }
}
