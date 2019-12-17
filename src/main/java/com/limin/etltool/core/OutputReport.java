package com.limin.etltool.core;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/17
 */
public interface OutputReport {

    void logSuccessResult(Object result);

    void logErrorResult(Object result);

    boolean hasError();

}
