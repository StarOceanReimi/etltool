package com.limin.etltool.redis;

import io.lettuce.core.api.sync.RedisCommands;

import java.util.Collection;
import java.util.List;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/25
 */
public interface RedisAccessor<T> {

    Collection<T> doWithCommands(RedisCommands<String, String> commands);
}
