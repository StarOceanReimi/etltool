package com.limin.etltool.step;

import com.limin.etltool.core.Transformer;
import com.limin.etltool.database.util.nameconverter.INameConverter;
import com.limin.etltool.util.Beans;

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/23
 */
public class MemoCacheTransformer<E>
        extends CachedBeanOperationTransform<Stream<E>, Stream<E>>
        implements Transformer<Stream<E>, Stream<E>> {

    private final String cidName;

    private final String pidName;

    private final BiConsumer<E, E> updater;

    private boolean cidReOrdered;

    public MemoCacheTransformer(String cidName, String pidName, BiConsumer<E, E> updater) {
        this.cidName = cidName;
        this.pidName = pidName;
        this.updater = updater;
    }

    public MemoCacheTransformer<E> cidReOrdered(boolean reOrdered) {
        this.cidReOrdered = reOrdered;
        return this;
    }

    private Object getVal(E v, String name) {
        Object result = null;
        if(v instanceof Map) {
            result = ((Map) v).get(name);
        } else {
            Beans.FastBeanOperation op = loadOperation(v);
            result = op.invokeGetter(v, name);
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private int comparator(E e1, E e2) {
        Comparable o1 = (Comparable) getVal(e1, cidName),
                o2 = (Comparable) getVal(e2, cidName);
        return o2.compareTo(o1);
    }

    @Override
    public Stream<E> transform(Stream<E> data) {

        if(cidReOrdered) data = data.sorted(this::comparator);
        Map<Object, E> memo = data.collect(toMap(o -> getVal(o, cidName),
                o -> o, (o1, o2) -> o1, LinkedHashMap::new));

        memo.forEach((key, cur) -> {
            E parent = memo.get(getVal(cur, pidName));
            updater.accept(cur, parent);
        });

        return memo.values().stream();
    }
}
