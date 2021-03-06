package com.limin.etltool.core;

import java.util.Collection;
import java.util.Objects;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/16
 */
public interface Transformer<T, R> extends Step {

    R transform(T data);

    default <V> Transformer<T, V> andThen(Transformer<R, V> transformer) {
        Objects.requireNonNull(transformer);
        return t -> transformer.transform(transform(t));
    }

    default <V> Transformer<V, R> andBefore(Transformer<V, T> transformer) {
        Objects.requireNonNull(transformer);
        return v -> transform(transformer.transform(v));
    }

}
