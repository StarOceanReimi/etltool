package com.limin.etltool.redis;

import com.google.common.base.Strings;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import lombok.*;

import java.util.List;
import java.util.regex.Pattern;

import static com.limin.etltool.util.TemplateUtils.logFormat;

/**
 * @author 邱理
 * @description
 * @date 创建于 2019/12/25
 */
@RequiredArgsConstructor
public class RedisConfiguration {

    @NonNull
    private String host;

    private int database = 0;

    private int port = 6379;

    @NonNull
    private String auth;

    private static final Pattern REDIS_URL_PATTERN =
            Pattern.compile("redis://(?<auth>.*)@?(?<host>.*):(?<port>.*)/(?<database>\\d{1,2})");

    private static final String URL_TPL = "redis://{}@{}:{}/{}";

    private String url;

    public String getUrl() {
        if(Strings.isNullOrEmpty(url)) {
            url = logFormat(URL_TPL, auth, host, port, database);
        }
        return url;
    }

    public static void main(String[] args) {

        RedisConfiguration configuration = new RedisConfiguration("192.168.137.57", "0LqWT5TZxYlttgKQZKqckF6LKGJlDoFnl6aGYSJQ29m3PD1UAFdKgAX0OJHUGLoZ");

        RedisClient client = RedisClient.create(configuration.getUrl());
        StatefulRedisConnection<String, String> redisConnection = client.connect();
        RedisCommands<String, String> commands = redisConnection.sync();
        List<String> keys = commands.keys("uname_to_access:*");
        keys.forEach(System.out::println);

        redisConnection.close();
        client.shutdown();


    }
}






















