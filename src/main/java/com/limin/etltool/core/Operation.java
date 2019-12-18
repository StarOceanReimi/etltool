package com.limin.etltool.core;

import com.google.common.collect.Lists;

import java.util.Collection;
import java.util.List;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/16
 */
public interface Operation<I, O> {

    Input<I> getInput();

    Output<O> getOutput();

    Transformer<I, O> getTransformer();

    /**
     * 处理从Input -> Transformer -> Output的流程
     * @param inputSource 输入源
     * @param outputSource 输出源
     * @throws EtlException 异常
     */
    void process(Source inputSource, Source outputSource) throws EtlException;
}
