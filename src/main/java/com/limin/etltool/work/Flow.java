package com.limin.etltool.work;

import com.google.common.base.Stopwatch;
import com.limin.etltool.core.*;
import com.limin.etltool.database.*;
import com.limin.etltool.database.mysql.DefaultMySqlDatabase;
import com.limin.etltool.step.ColumnEditing;
import com.limin.etltool.step.ColumnMapping;
import com.limin.etltool.step.GroupByFieldWithCondition;
import com.limin.etltool.step.GroupByOperationMapping;
import lombok.val;

import java.time.LocalDateTime;
import java.util.Arrays;
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



        DatabaseConfiguration configuration = new DatabaseConfiguration("classpath:database.yml");
        Database database = new DefaultMySqlDatabase(configuration.databaseName("dangjian"));
        DatabaseAccessor accessor = new TableColumnAccessor("fact_party_org").column("id", "category");
        DbInput<Map<String, Object>> input = new NormalDbInput<Map<String, Object>>(database, accessor) {};

        GroupByFieldWithCondition<Map<String, Object>> conditionedGroup = new GroupByFieldWithCondition<>();
        List<Long> dwList   = Arrays.asList(10000141L, 10000142L);
        List<Long> dzzList  = Arrays.asList(10000143L, 10000144L);
        conditionedGroup
                .addMapping("category", "dw", (m) -> dwList.contains(m.get("category")))
                .addMapping("category", "dzz", (m) -> dzzList.contains(m.get("category")));

        GroupByOperationMapping<Map<String, Object>, Map<String, Object>> mapping =
                new GroupByOperationMapping<>()
                        .nullValueHandler(() -> 0L)
                        .addMapping("category", "count", Collectors.counting());


        conditionedGroup
                .andThen(mapping)
                .transform(input.readCollection().stream())
                .forEach(System.out::println);

    }
}
