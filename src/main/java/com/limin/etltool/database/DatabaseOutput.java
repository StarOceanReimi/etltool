package com.limin.etltool.database;

import com.limin.etltool.core.Output;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/16
 */
public interface DatabaseOutput<T> extends Output<T> {

    DatabaseOutputType getDatabaseOutputType();

}
