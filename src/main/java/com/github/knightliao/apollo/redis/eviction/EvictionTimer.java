package com.github.knightliao.apollo.redis.eviction;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * EvictionTimer
 *
 * @author zhangxu04
 */
public class EvictionTimer {

    private static Timer timer;
    private static AtomicInteger usageCount = new AtomicInteger(0);

    private EvictionTimer() {
        // Hide the default constuctor
    }

    /**
     * Add the specified eviction task to the timer. Tasks that are added with a call to this method *must* call
     * {@link #cancel(java.util.TimerTask)} to cancel the task to prevent memory and/or thread leaks in application
     * server
     * environments.
     *
     * @param task                      Task to be scheduled
     * @param evictorDelayCheckSeconds  Delay in milliseconds before task is executed
     * @param evictorCheckPeriodSeconds Time in milliseconds between executions
     */
    public static synchronized void schedule(TimerTask task, int evictorDelayCheckSeconds,
                                             int evictorCheckPeriodSeconds) {
        if (null == timer) {
            timer = new Timer(true);
        }
        usageCount.incrementAndGet();
        timer.schedule(task, evictorDelayCheckSeconds * 1000, evictorCheckPeriodSeconds * 1000);
    }

    /**
     * Remove the specified eviction task from the timer.
     *
     * @param task Task to be scheduled
     */
    public static synchronized void cancel(TimerTask task) {
        if (task == null) {
            return;
        }
        task.cancel();
        usageCount.decrementAndGet();
        if (usageCount.get() == 0) {
            timer.cancel();
            timer = null;
        }
    }
}
