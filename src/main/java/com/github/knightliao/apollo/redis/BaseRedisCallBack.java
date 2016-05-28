package com.github.knightliao.apollo.redis;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.knightliao.apollo.redis.exception.RedisOperationException;
import com.github.knightliao.apollo.redis.util.RandomUtil;

/**
 * 提供高可用特性的Redis客户端调用回调抽象类
 */
public abstract class BaseRedisCallBack<T> implements RedisCallBack<T> {

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * 调用异常
     */
    private Exception e;

    /**
     * 调用结果
     */
    private T result;

    /**
     * 调用redis
     *
     * @param client
     *
     * @return
     *
     * @throws Exception
     */
    protected abstract T doOperation(RedisClient client) throws Exception;

    /**
     * 实现多写，随机读策略的模板方法
     * <p/>
     * 如果为写请求，则尝试多写，至少一个成功返true，全部失败返回false <br>
     * 如果为读请求，则尝试随机读，读到空或者异常继续，直到读到非空结果，返回true，否则全部尝试失败返回false
     *
     * @see RedisCallBack#doInRedis(java.util.List, boolean, Object)
     */
    public final boolean doInRedis(List<RedisClient> clients, boolean isRead, Object key) {
        List<Integer> randomIndexs = RandomUtil.randomizeWithinLimit(clients.size());
        boolean success = false;
        for (Integer index : randomIndexs) {
            RedisClient client = clients.get(index);
            long start = System.currentTimeMillis();
            try {
                result = doOperation(client);
                long end = System.currentTimeMillis();
                logger.info("[RedisCache:" + getOptionType() + "]" + " <key:" + key + "> <client: " +
                        client.getCacheName() + "> <server: " + client.getLiteralRedisServer() +
                        "> success ! (use " + (end - start) + " ms)");
                if (isRead) { // read=true，读取出非空即返回，否则双写
                    if (result == null) {
                        // retry another client
                        logger.info("[RedisCache:" + getOptionType() + "]" + " <key:" + key + "> <client: " +
                                client.getCacheName() + "> <server: " + client.getLiteralRedisServer() +
                                "> but null result... (use " + (end - start) + " ms)");
                        continue;
                    }
                    return true;
                }
                success = success || true;
            } catch (Exception e) {
                success = success || false;
                this.e = new RedisOperationException(e.getMessage() + "@" + client.getLiteralRedisServer(), e);
                long end = System.currentTimeMillis();
                logger.error("[[RedisCache:" + getOptionType() + "]" + " <key:" + key + "> <client: " +
                        client.getCacheName() + "> <server: " + client.getLiteralRedisServer() + "> fail. " +
                        e.getMessage() + "! (use " + (end - start) + " ms)");
            }
        }

        return success;
    }

    public T getResult() {
        return result;
    }

    public Exception getException() {
        return e;
    }
}
