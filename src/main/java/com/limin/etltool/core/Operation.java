package com.limin.etltool.core;

import com.google.common.collect.Lists;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/16
 */
public interface Operation<I, O> {

    default void process(Input<I> input, Transformer<I, O> transformer, Output<O> output) throws EtlException {
        Collection<I> inputCollection = input.readCollection();
        List<O> buffer = inputCollection.stream()
                .map(transformer::transform)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        output.writeCollection(buffer);
    }
}
