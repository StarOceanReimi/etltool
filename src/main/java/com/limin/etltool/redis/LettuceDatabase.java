package com.limin.etltool.redis;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/25
 */
public class LettuceDatabase {

    private final RedisClient redisClient;

    public LettuceDatabase(RedisConfiguration configuration) {
        this.redisClient = RedisClient.create(configuration.getUrl());
    }

    public StatefulRedisConnection<String, String> getConnection() {
        return redisClient.connect();
    }

    public void shutdown() {
        redisClient.shutdown();
    }
}
