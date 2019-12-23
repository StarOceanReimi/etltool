package com.limin.etltool.database.util.nameconverter;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/17
 */
public class NopNameConverter implements INameConverter {

    @Override
    public String rename(String name) {
        return name;
    }

    @Override
    public INameConverter getReverse() {
        return this;
    }


}
