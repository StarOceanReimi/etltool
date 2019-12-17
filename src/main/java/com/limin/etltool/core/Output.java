package com.limin.etltool.core;

import java.util.Collection;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/16
 */
public interface Output {

    /**
     * 使用输出源消费数据集合
     * @param dataCollection 数据集合
     * @param outputSource 输出源
     * @param <T> 数据类型
     * @return 是否成功
     */
    <T> boolean writeCollection(Collection<T> dataCollection, Source outputSource);
}
