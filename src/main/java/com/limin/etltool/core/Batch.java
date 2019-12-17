package com.limin.etltool.core;

import java.util.List;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/17
 */
public interface Batch<T> {

    boolean hasMore();

    List<T> getMore();

    void release();
}
