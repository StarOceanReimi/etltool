package com.limin.etltool.database;

import java.util.List;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/17
 */
public interface DatabaseProperties {

    String getTable();

    List<String> getColumns();

}
