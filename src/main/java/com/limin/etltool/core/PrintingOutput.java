package com.limin.etltool.core;

import java.util.Collection;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/20
 */
public class PrintingOutput<T> implements Output<T> {

    @Override
    public boolean writeCollection(Collection<T> dataCollection) throws EtlException {
        dataCollection.forEach(System.out::println);
        return true;
    }

}
