package com.limin.etltool.core;

import org.junit.Test;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 *
 * </p>
 *
 * @author 邱理 WHRDD-PC104
 * @since 2020/8/11
 */
public class SchedulePoolTest {

    @Test
    public void test() throws InterruptedException {

        ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(4);

        executorService.execute(() -> System.out.println(Thread.currentThread().getName() + ": Hello!"));

        executorService.shutdown();

        executorService.awaitTermination(1000, TimeUnit.MINUTES);

        System.out.println("Finished");

    }
}
