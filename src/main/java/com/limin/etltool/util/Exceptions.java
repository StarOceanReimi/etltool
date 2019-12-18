package com.limin.etltool.util;

import com.limin.etltool.core.EtlException;
import lombok.extern.slf4j.Slf4j;

import java.util.Objects;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/17
 */
@Slf4j
public abstract class Exceptions {

    public static void rethrow(Throwable throwable) {
        throw new RuntimeEtlException(throwable);
    }

    public static RuntimeEtlException propagate(Throwable throwable) {
        return new RuntimeEtlException(throwable);
    }

    public static RuntimeEtlException inform(String message, Object... args) {
        return new RuntimeEtlException(TemplateUtils.logFormat(message, args));
    }

    public static UnsupportedOperationException unsupported(String message, Object... args) {
        return new UnsupportedOperationException(TemplateUtils.logFormat(message, args));
    }
}
