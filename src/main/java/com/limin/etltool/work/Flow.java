package com.limin.etltool.work;

import com.limin.etltool.core.*;
import com.limin.etltool.database.*;
import com.limin.etltool.database.mysql.DefaultMySqlDatabase;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author 邱理
 * @description
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

    public static void main(String[] args) throws EtlException {
        String sql = "SELECT 1 rownum, gau.area_id, gau.user_id, gau.is_party_leader, gau.is_party_first, gau.is_people_leader, gau.is_people_first, u.sex, ud.politics_status, ud.birthday, ud.education, TIMESTAMPDIFF(YEAR, ud.birthday, CURDATE()) as age FROM ( SELECT au.area_id, au.user_id, max(au.is_party_leader) AS is_party_leader, max(au.is_party_first) AS is_party_first, max(au.is_people_leader) AS is_people_leader, max(au.is_people_first) AS is_people_first FROM ( SELECT area.area_id, p.unit_id AS fact_unit_id, partyup.user_id, 1 AS is_party_leader, CASE WHEN p.unit_id = area.party_unit_id AND partyp.post_sequence IN (100051, 100059, 100062) THEN 1 ELSE 0 END is_party_first, 0 AS is_people_leader, 0 AS is_people_first FROM ( SELECT ad.id area_id, partyUnit.id party_unit_id, p.permission_id FROM uc_info_area_detail ad INNER JOIN uc_info_area a ON a.id = ad.id INNER JOIN uc_unit partyUnit ON ad.unit_id = partyUnit.id AND partyUnit.is_deleted = 0 INNER JOIN common_query.uc_permission p ON partyUnit.id = p.unit_id WHERE a.`level` = 5 ) area INNER JOIN common_query.uc_permission p ON p.permission_id LIKE CONCAT(area.permission_id, '%') INNER JOIN uc_post partyp ON partyp.unit_id = p.unit_id AND partyp.post_sequence IS NOT NULL AND partyp.post_sequence <= 100069 AND partyp.is_deleted = 0 INNER JOIN uc_user_post partyup ON partyup.post_id = partyp.id and partyup.start_time <= NOW() and NOW() <= partyup.end_time and partyup.is_deleted = 0 UNION ALL SELECT peopleUnit.area_id, peopleUnit.id AS fact_unit_id, peopleup.user_id, 0 AS is_party_leader, 0 AS is_party_first, 1 AS is_people_leader, CASE WHEN peoplep.post_sequence = 10200502 THEN 1 ELSE 0 END is_people_first FROM uc_unit_people peopleUnit INNER JOIN uc_info_area a ON a.id = peopleUnit.area_id INNER JOIN uc_post peoplep ON peoplep.unit_id = peopleUnit.id AND peoplep.post_sequence IS NOT NULL AND peoplep.is_deleted = 0 INNER JOIN uc_user_post peopleup ON peopleup.post_id = peoplep.id and peopleup.start_time <= NOW() and NOW() <= peopleup.end_time and peopleup.is_deleted = 0 WHERE a.`level` = 5 ) au GROUP BY area_id, user_id ) gau INNER JOIN uc_user u ON u.gau.user_id = u.id AND u.is_deleted = 0 INNER JOIN uc_user_detail ud ON ud.id = u.id";

        Flow<Map<String, Object>, Map<String, Object>> flow = new Flow<>();
        DefaultMySqlDatabase inputDatabase = new DefaultMySqlDatabase(new DatabaseConfiguration("classpath:database.yml"));
        DatabaseAccessor databaseAccessor = new DefaultDatabaseAccessor(sql);
        Input input = new NormalDbInput(inputDatabase, databaseAccessor) {};
        DefaultMySqlDatabase outputDatabase = new DefaultMySqlDatabase(new DatabaseConfiguration("classpath:database1.yml"));
        DatabaseAccessor outputAccessor = new TableColumnAccessor(TableColumnAccessor.SqlType.INSERT, "fact_village_cadre");

        Output output = new NormalDbOutput(outputDatabase, outputAccessor) {};

    }
}
