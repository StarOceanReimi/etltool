package com.limin.etltool.work;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.limin.etltool.core.*;
import com.limin.etltool.database.*;
import com.limin.etltool.database.mysql.DefaultMySqlDatabase;
import com.limin.etltool.step.*;
import lombok.val;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author 邱理
 * @description 定义普通流转
 * @date 创建于 2019/12/19
 */
public class Flow<I, O> implements Operation<I, O> {

    public void processInBatch(int batchSize, BatchInput<I> input, Transformer<Stream<I>, Stream<O>> t, Output<O> output) throws EtlException {
        Batch<I> batch = input.readInBatch(batchSize);
        try {
            while (batch.hasMore()) {
                List<I> data = batch.getMore();
                output.writeCollection(t.transform(data.stream()).collect(Collectors.toList()));
            }
        } finally {
            batch.release();
        }
    }

    private static void moveStudyRecordTest() throws EtlException {

        String sql = "select id, user_id, unit_id, content_id, start_time, end_time, dif_time study_time from edu_learn_note";
        DatabaseConfiguration inputConfig = new DatabaseConfiguration("classpath:database.yml");
        DefaultMySqlDatabase inputDatabase = new DefaultMySqlDatabase(inputConfig.databaseName("edu_online"));
        DatabaseAccessor inputAccessor = new DefaultDatabaseAccessor(sql);

        NormalDbInput<Map<String, Object>> input = new NormalDbInput<Map<String, Object>>(inputDatabase, inputAccessor) {};

        DatabaseConfiguration outputConfig = new DatabaseConfiguration("classpath:database1.yml");
        DefaultMySqlDatabase outputDatabase = new DefaultMySqlDatabase(outputConfig);
        DatabaseAccessor outputAccessor = new TableColumnAccessor(TableColumnAccessor.SqlType.INSERT, "fact_study_record_test");
        NormalDbOutput<Map<String, Object>> output = new NormalDbOutput<Map<String, Object>>(outputDatabase, outputAccessor) {};
        output.setCreateTableIfNotExists(true);

        ColumnMapping mapping = new ColumnMapping();
        mapping.addColumnForMapBean("ts");
        ColumnEditing<Map<String, Object>> editor = new ColumnEditing<>();
        editor.registerEditor("ts", (m)->m.put("ts", LocalDateTime.now()));

        val flow = new Flow<Map<String, Object>, Map<String, Object>>();
        val sw = Stopwatch.createStarted();
        flow.processInBatch(4096, input, mapping.andThen(editor), output);
        System.out.println(sw.stop());

    }



    public static void main(String[] args) throws Exception {

//        moveStudyRecordTest();


        DatabaseConfiguration configuration = new DatabaseConfiguration("classpath:database.yml");
        Database database = new DefaultMySqlDatabase(configuration.databaseName("dangjian"));
        DatabaseAccessor accessor = new TableColumnAccessor("fact_party_org").column("id", "parent_id", "category");
        DbInput<Map<String, Object>> input = new NormalDbInput<Map<String, Object>>(database, accessor) {};

        MemoCacheTransformer<Map<String, Object>> memoCacheTransformer =
                new MemoCacheTransformer<>("id", "parent_id", (me, parent)-> {
                    me.putIfAbsent("children_count", 1L);
                    me.putIfAbsent("children_categories", Lists.newArrayList());
                    ((List)me.get("children_categories")).add(me.get("category"));
                    if(parent == null) return;
                    Long children = (Long) parent.getOrDefault("children_count", 1L);
                    List<Object> childrenCategories = (List<Object>) parent.getOrDefault("children_categories", Lists.newArrayList());
                    childrenCategories.addAll((Collection<?>) me.get("children_categories"));
                    parent.putIfAbsent("children_categories", childrenCategories);
                    parent.put("children_count", children + (Long) me.get("children_count"));
                });
        memoCacheTransformer.cidReOrdered(true);



//        GroupByField<Map<String, Object>> groupByField = new GroupByField<>("category");
//
//        GroupByOperationMapping<Map<String, Object>, Map<String, Object>> operationMapping =
//                new GroupByOperationMapping<>()
//                    .addMapping("children_count", "sum", Collectors.summingLong(o -> (Long) o));

//        memoCacheTransformer
//                .andThen(groupByField).andThen(operationMapping)
//                .transform(input.readCollection().stream())
//                .forEach(System.out::println);

//        input.readCollection().forEach(System.out::println);
    }

}
