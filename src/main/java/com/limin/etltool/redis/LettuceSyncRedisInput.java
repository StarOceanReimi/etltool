package com.limin.etltool.redis;

import com.limin.etltool.core.EtlException;
import com.limin.etltool.core.Input;
import io.lettuce.core.api.StatefulRedisConnection;

import java.util.Collection;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/25
 */
public class LettuceSyncRedisInput<T> implements Input<T>, AutoCloseable {

    private final StatefulRedisConnection<String, String> redisConnection;

    private final RedisAccessor<T> accessor;

    private LettuceDatabase database;

    public LettuceSyncRedisInput(LettuceDatabase database, RedisAccessor<T> accessor) {
        this.database = database;
        this.redisConnection = database.getConnection();
        this.accessor = accessor;
    }

    @Override
    public Collection<T> readCollection() throws EtlException {
        return accessor.doWithCommands(redisConnection.sync());
    }

    @Override
    public void close() throws Exception {
        redisConnection.close();
        database.shutdown();

    }

    public static void main(String[] args) throws EtlException {


    }
}
