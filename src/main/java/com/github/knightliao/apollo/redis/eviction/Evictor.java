package com.github.knightliao.apollo.redis.eviction;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.knightliao.apollo.redis.RedisClient;

/**
 * redis连接失效检测器，具备失效连接的自动剔除以及自动恢复
 * <p/>
 * 按照时间间隔遍历所有redis client，进行ping，如果返回pong，则证明连接可用。 如果出现异常pong没有返回，则代表失效连接一次，连续到达指定的失败次数后，标记redis client为失效，置alive为false。
 * 当ping-pong再次响应正常后，即恢复redis client为可用，重置alive为true。
 */
public class Evictor extends TimerTask {

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * redis ping-pong 响应常量
     */
    private static final String PONG = "PONG";

    /**
     * 检测的redis client列表
     */
    private List<RedisClient> clientList;

    /**
     * 最多允许连续失败次数
     */
    private int evictorFailedTimesToBeTickOut;

    /**
     * redis client到连接失败次数的映射Map
     */
    private Map<RedisClient, AtomicInteger> evictorFailedTimesCount = new HashMap<RedisClient, AtomicInteger>();

    /**
     * Creates a new instance of Evictor.
     *
     * @param clientList
     * @param evictorFailedTimesToBeTickOut
     */
    public Evictor(List<RedisClient> clientList, int evictorFailedTimesToBeTickOut) {
        super();
        this.clientList = clientList;
        this.evictorFailedTimesToBeTickOut = evictorFailedTimesToBeTickOut;
        for (RedisClient redisClient : clientList) {
            evictorFailedTimesCount.put(redisClient, new AtomicInteger(0));
        }
    }

    /**
     * 运行检测器
     *
     * @see java.util.TimerTask#run()
     */
    @Override
    public void run() {
        try {
            evict();
        } catch (Exception e) {
            // ignored
        }
    }

    /**
     * 失效连接自动剔除/恢复
     */
    private synchronized void evict() {
        for (RedisClient client : clientList) {
            if (!PONG.equals(client.ping())) {
                if (!client.isAlive()) {
                    logger.debug("ping not get pong and client still not alive, " + client.getLiteralRedisServer());
                    continue;
                }
                int failedTimes = evictorFailedTimesCount.get(client).incrementAndGet();
                logger.warn("failed times: " + failedTimes +
                        ", ping not get pong but client failed times is not more than " +
                        evictorFailedTimesToBeTickOut + ", so client is alive " +
                        client.getLiteralRedisServer());
                if (failedTimes > evictorFailedTimesToBeTickOut) {
                    logger.error("failed times is bigger than " + evictorFailedTimesToBeTickOut +
                            ", so client tick out, " + client.getLiteralRedisServer());
                    client.setAlive(false);
                }
            } else {
                if (evictorFailedTimesCount.get(client).get() == 0) {
                    logger.debug("ping get pong and last ping-pong is ok, " + client.getLiteralRedisServer());
                    continue;
                }
                logger
                        .warn("last ping-pong is failed, this is first time to recover, " + client
                                .getLiteralRedisServer());
                evictorFailedTimesCount.get(client).set(0);
                client.setAlive(true);
            }
        }
    }

}
