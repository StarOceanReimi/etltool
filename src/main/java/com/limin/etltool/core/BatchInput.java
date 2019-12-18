package com.limin.etltool.core;

import com.google.common.collect.Lists;

import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

import static com.limin.etltool.util.Exceptions.rethrow;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/17
 */
public interface BatchInput<T> extends Input<T> {

    int DEFAULT_BATCH_SIZE = 1024;

    Batch<T> readInBatch(int batchSize) throws EtlException;

    default void consumeBatch(int batchSize, Consumer<Collection<T>> batchConsumer) {
        Batch<T> batch = null;
        try {
            batch = readInBatch(batchSize);
            while (batch.hasMore())
                batchConsumer.accept(batch.getMore());
        } catch (EtlException ex) {
            rethrow(ex);
        } finally {
            if(batch != null) batch.release();
        }

    }

    @Override
    default Collection<T> readCollection() throws EtlException {
        List<T> result = Lists.newArrayList();
        consumeBatch(DEFAULT_BATCH_SIZE, result::addAll);
        return result;
    }
}
