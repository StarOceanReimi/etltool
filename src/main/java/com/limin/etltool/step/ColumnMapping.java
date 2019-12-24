package com.limin.etltool.step;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.limin.etltool.core.EtlException;
import com.limin.etltool.core.Transformer;
import com.limin.etltool.database.*;
import com.limin.etltool.database.mysql.DefaultMySqlDatabase;
import com.limin.etltool.util.Beans;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.beanutils.PropertyUtils;

import java.beans.PropertyDescriptor;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Optional.ofNullable;

/**
 * @author 邱理
 * @description 流字段映射
 * @date 创建于 2019/12/19
 */
@Slf4j
public class ColumnMapping<T1, T2> extends CachedBeanOperationTransform<Stream<T1>, Stream<T2>> implements Transformer<Stream<T1>, Stream<T2>> {

    private Map<String, String> columnMapping = Maps.newHashMap();

    private Set<String> extraColumnNames = Sets.newHashSet();

    private final Supplier<T2> outputBeanSupplier;

    private boolean retainUnmapped = true;

    public ColumnMapping() {
        this(null);
    }

    public ColumnMapping<T1, T2> retainUnmapped(boolean retainUnmapped) {
        this.retainUnmapped = retainUnmapped;
        return this;
    }

    @SuppressWarnings("unchecked")
    public ColumnMapping(Supplier<T2> outputBeanSupplier) {
        if(outputBeanSupplier == null)
            this.outputBeanSupplier = () -> (T2) new HashMap<String, Object>();
        else
            this.outputBeanSupplier = outputBeanSupplier;
    }

    public ColumnMapping<T1, T2> addColumnForMapBean(String extraColumn) {
        extraColumnNames.add(extraColumn);
        return this;
    }

    public ColumnMapping<T1, T2> addMapping(String inputColumn, String outputColumn) {
        columnMapping.put(inputColumn, outputColumn);
        return this;
    }

    @SuppressWarnings("unchecked")
    private void setValueForTarget(Object target, String name, Object value) {
        if(target instanceof Map) {
            Map<String, Object> outputMap = (Map<String, Object>) target;
            outputMap.put(name, value);
            extraColumnNames.forEach(c -> outputMap.put(c, null));
        } else {
            Beans.FastBeanOperation targetOp = loadOperation(target);
            targetOp.invokeSetter(target, name, value);
        }
    }

    protected T2 innerTransform(T1 data) {
        T2 target = outputBeanSupplier.get();
        if(data instanceof Map) {
            Map<String, Object> inputMap = (Map<String, Object>) data;
            for(String input : inputMap.keySet()) {
                String output = ofNullable(columnMapping.get(input)).orElse(retainUnmapped ? input : null);
                if(output == null) continue;
                Object value = inputMap.get(input);
                setValueForTarget(target, output, value);
            }
        } else {
            PropertyDescriptor[] descs = PropertyUtils.getPropertyDescriptors(data.getClass());
            Beans.FastBeanOperation dataOp = loadOperation(data);
            for(PropertyDescriptor desc : descs) {
                String input = desc.getDisplayName();
                if("class".equals(input)) continue;
                String output = ofNullable(columnMapping.get(input)).orElse(retainUnmapped ? input : null);
                if(output == null) continue;
                Object value = dataOp.invokeGetter(data, input);
                setValueForTarget(target, output, value);
            }
        }
        return target;
    }

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws EtlException {
        DatabaseConfiguration configuration = new DatabaseConfiguration();
        Database database = new DefaultMySqlDatabase(configuration);
        DatabaseAccessor accessor = new TableColumnAccessor("co_comment").column("id", "body");
        val input = new NormalDbInput<Map<String, Object>>(database, accessor) {};

        val collection = input.readCollection();
        ColumnMapping<Map<String, Object>, Map<String, Object>> columnMapping = new ColumnMapping<>();
        columnMapping.addMapping("id", "sid").addMapping("body", "content").addColumnForMapBean("fake");

        ColumnEditing<Map<String, Object>> editor = new ColumnEditing<>();
        editor.registerEditor("fake", (m) -> m.put("fake", ThreadLocalRandom.current().nextInt()));

        GroupByField<Map<String, Object>> byField = new GroupByField<>("content");

        GroupByOperationMapping<Map<String, Object>, Map<String, Object>> groupByOperationMapping =
                new GroupByOperationMapping<>()
                        .addMapping("content", "count", Collectors.counting())
                        .addMapping("content", "sumLength",
                                Collectors.summingLong((String content) -> Strings.isNullOrEmpty(content) ? 0 : content.length())) ;

        columnMapping.andThen(editor).andThen(byField).andThen(groupByOperationMapping)
                .transform(collection.stream()).forEach(System.out::println);

    }

    @Override
    public Stream<T2> transform(Stream<T1> data) {
        return data.map(this::innerTransform);
    }
}
