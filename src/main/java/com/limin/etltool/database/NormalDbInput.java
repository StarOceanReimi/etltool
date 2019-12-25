package com.limin.etltool.database;

import com.limin.etltool.core.EtlException;
import com.limin.etltool.database.business.LatestModifyAccessor;
import com.limin.etltool.database.mysql.ColumnDefinitionHelper;
import com.limin.etltool.database.mysql.DefaultMySqlDatabase;
import lombok.val;

import java.util.Map;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/18
 */
public class NormalDbInput<T> extends AbstractDbInput<T> {

    protected NormalDbInput(Database database, DatabaseAccessor accessor) {
        super(null, database, accessor);
    }

    public NormalDbInput(Class<T> componentType, Database database, DatabaseAccessor accessor) {
        super(componentType, database, accessor);
    }

    public static void main(String[] args) throws EtlException {

        String sql = "SELECT r.*,\n" +
                "r1.all_agency_count,\n" +
                "r1.expire_agency_count,\n" +
                "r1.finish_agency_count,\n" +
                "r1.having_agency_count,\n" +
                "r1.unstart_agency_count from (SELECT\n" +
                "  t.unit_id,\n" +
                "  t.unit_name,\n" +
                "  '0' task_type,\n" +
                "  CONVERT(sum(CASE WHEN t.state IN (0, 1, 2, 3) THEN 1 ELSE 0 END), UNSIGNED) AS all_supervise_count,\n" +
                "  sum(CASE t.state WHEN 0 THEN 1 ELSE 0 END) AS unstart_supervise_count,\n" +
                "  sum(CASE t.state WHEN 1 THEN 1 ELSE 0 END) AS having_supervise_count,\n" +
                "  sum(CASE t.state WHEN 2 THEN 1 ELSE 0 END) AS finish_supervise_count,\n" +
                "  sum(CASE t.state WHEN 3 THEN 1 ELSE 0 END) AS expire_supervise_count,\n" +
                "  now() modify_at\n" +
                "FROM\n" +
                "  (\n" +
                "    SELECT\n" +
                "      uu.id unit_id,\n" +
                "      uu.`name` unit_name,\n" +
                "      (\n" +
                "        CASE \n" +
                "        WHEN ISNULL(fwt.state) THEN - 1\n" +
                "        WHEN fwt.state = 1 AND now() < fwt.end_time AND fwt.start_time < now() THEN 1\n" +
                "        WHEN fwt.state = 1 AND now() < fwt.start_time THEN 0 WHEN fwt.state = 1 AND fwt.end_time < now() THEN 3\n" +
                "        ELSE fwt.state\n" +
                "        END\n" +
                "      ) AS state     \n" +
                "    FROM\n" +
                "      fact_party_org uu\n" +
                "    LEFT JOIN fact_work_task fwt ON uu.id = fwt.creater_unit_id and fwt.is_deleted=0\n" +
                "    AND date_format(fwt.start_time, '%Y-%m-%d') <= date_format(now(), '%Y-%m-%d')\n" +
                "    AND date_format(now(), '%Y-%m-%d') <= date_format(fwt.end_time, '%Y-%m-%d')\n" +
                "    where uu.is_deleted=0\n" +
                "  ) t\n" +
                "GROUP BY t.unit_Id) r\n" +
                "LEFT JOIN (select \n" +
                "  uu.id unit_id,\n" +
                "  sum(CASE WHEN wt.state IN (0, 1, 2, 3) THEN 1 ELSE 0 END) AS all_agency_count,\n" +
                "  sum(CASE wt.state WHEN 0 THEN 1 ELSE 0 END) AS unstart_agency_count,\n" +
                "  sum(CASE wt.state WHEN 1 THEN 1 ELSE 0 END) AS having_agency_count,\n" +
                "  sum(CASE wt.state WHEN 2 THEN 1 ELSE 0 END) AS finish_agency_count,\n" +
                "  sum(CASE wt.state WHEN 3 THEN 1 ELSE 0 END) AS expire_agency_count\n" +
                " FROM\n" +
                "      fact_party_org uu\n" +
                "LEFT JOIN (\n" +
                "SELECT\n" +
                "  fwtu.unit_id,\n" +
                "  fwtu.work_task_id,\n" +
                "  (\n" +
                "        CASE \n" +
                "        WHEN ISNULL(fwtu.state) THEN - 1\n" +
                "        WHEN fwtu.state = 1 AND now() < fwt.end_time AND fwt.start_time < now() THEN 1\n" +
                "        WHEN fwtu.state = 1 AND now() < fwt.start_time THEN 0 WHEN fwt.state = 1 AND fwt.end_time < now() THEN 3\n" +
                "        ELSE fwtu.state\n" +
                "        END\n" +
                "      ) AS state, \n" +
                "  fwt.start_time,\n" +
                "  fwt.end_time\n" +
                "FROM\n" +
                "  fact_work_task_unit fwtu\n" +
                "JOIN fact_work_task fwt ON fwt.id = fwtu.work_task_id\n" +
                "AND fwt.is_deleted = 0   \n" +
                "AND date_format(fwt.start_time, '%Y-%m-%d') <= date_format(now(), '%Y-%m-%d')\n" +
                "AND date_format(now(), '%Y-%m-%d') <= date_format(fwt.end_time, '%Y-%m-%d')\n" +
                "WHERE\n" +
                "  fwtu.is_deleted = 0) wt on wt.unit_id = uu.id\n" +
                "where uu.is_deleted =0\n" +
                "GROUP BY uu.id) r1 ON r1.unit_id = r.unit_id";

        DatabaseAccessor accessor = new DefaultDatabaseAccessor(sql);
//                new LatestModifyAccessor("co_comment");
//                new TableColumnAccessor(TableColumnAccessor.SqlType.SELECT, "my_test");
        DatabaseConfiguration configuration = new DatabaseConfiguration("classpath:database1.yml");
        Database database = new DefaultMySqlDatabase(configuration);
        val input = new NormalDbInput<Map<String, Object>>(database, accessor) {} ;
        val result = input.readCollection();

        val sample = result.iterator().next();

        System.out.println(ColumnDefinitionHelper.fromMap(sample));

    }
}
