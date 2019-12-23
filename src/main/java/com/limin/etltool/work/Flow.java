package com.limin.etltool.work;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Maps;
import com.limin.etltool.core.*;
import com.limin.etltool.database.*;
import com.limin.etltool.database.mysql.DefaultMySqlDatabase;
import com.limin.etltool.step.ColumnEditing;
import com.limin.etltool.step.ColumnMapping;
import com.limin.etltool.step.GroupByField;
import com.limin.etltool.step.GroupByFieldReducer;
import lombok.val;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author 邱理
 * @description 定义普通流转
 * @date 创建于 2019/12/19
 */
public class Flow<I, O> implements Operation<I, O> {

    public void processInBatch(int batchSize, BatchInput<I> input, Transformer<I, O> t, Output<O> output) throws EtlException {
        Batch<I> batch = input.readInBatch(batchSize);
        try {
            while (batch.hasMore()) {
                List<I> data = batch.getMore();
                List<O> list = data.stream()
                        .map(t::transform)
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());
                output.writeCollection(list);
            }
        } finally {
            batch.release();
        }
    }

    private void moveStudyRecordTest() throws EtlException {

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

        GroupByField<Map<String, Object>> groupField = new GroupByField<>("category");


    }
}
