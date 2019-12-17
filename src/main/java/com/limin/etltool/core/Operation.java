package com.limin.etltool.core;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/16
 */
public interface Operation {

    /**
     * 处理从Input -> Output的流程
     * @param input 输入
     * @param output 输出
     * @throws EtlException
     */
    void process(Input input, Output output) throws EtlException;
}
