package com.limin.etltool.core;

import com.google.common.collect.Lists;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/21
 */
public abstract class CombinedInput<T> implements BatchInput<T> {

    protected List<Input> inputsList;

    public CombinedInput(Input... inputs) {
        checkArgument(inputs != null && inputs.length > 0, "must have at least one input.");
        inputsList = Lists.newArrayList(Arrays.asList(inputs));
    }

    protected abstract Collection<T> combineInput() throws EtlException;

    @Override
    public Batch<T> readInBatch(int batchSize) throws EtlException {
        return new SingleBatch();
    }

    class SingleBatch implements Batch<T> {

        private boolean first = true;

        @Override
        public boolean hasMore() {
            return first;
        }

        @Override
        public List<T> getMore() {
            try {
                Collection<T> collection = combineInput();
                first = false;
                if(collection instanceof List) return (List<T>) collection;
                return Lists.newArrayList(collection);
            } catch (EtlException e) {
                return Collections.emptyList();
            }
        }

        @Override
        public void release() { }
    }
}
