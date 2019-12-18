package com.limin.etltool.core;

import java.util.Collection;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/16
 */
public interface Input<T> extends Source {

    /**
     * 从输入源读取数据集合
     * @return 数据集合
     * @throws EtlException
     */
    Collection<T> readCollection() throws EtlException;

}
