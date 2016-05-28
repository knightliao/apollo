package com.github.knightliao.apollo.redis;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.knightliao.apollo.redis.eviction.EvictionTimer;
import com.github.knightliao.apollo.redis.eviction.Evictor;
import com.github.knightliao.apollo.redis.exception.RedisOperationException;

/**
 * ClassName: RedisCacheManager <br>
 * Function: 提供高可用特性的Redis client管理类，针对一组redis server节点配置为单例模式使用，供其他组件使用。
 * <p/>
 * 通过维护一组{@link RedisClient RedisClient}实例，来进行多写随机读策略，具体策略请参考
 * {@link BaseRedisCallBack BaseRedisCallBack}。 对于每个{@link RedisClient
 * RedisClient}实例的连接池配置不在本管理器中维护，而是由jedis客户端维护。 管理器提供失效连接自动剔除以及自动恢复策略，具体策略请参考
 * {@link com.baidu.unbiz.redis.eviction.Evictor Evictor}。
 * <p/>
 * 本管理类可以通过两种方式来使用：<br>
 * 1) 直接API
 * <p/>
 * <pre>
 * RedisCacheManager redisMgr = RedisCacheManager.of(getRedisClientList()).buildRetryTimes(1)
 *         .buildEvictorCheckPeriodSeconds(10).buildEvictorDelayCheckSeconds(5).buildEvictorFailedTimesToBeTickOut(3);
 *
 * redisMgr.init(); // 如需要启动失效自动剔除恢复检测器，请务必手工调用init方法。
 * </pre>
 * <p/>
 * 2) 使用spring xml配置，直接注入{@link RedisClient RedisClient}， 同时如需要启动失效自动剔除恢复检测器，请务必加入
 * <code>init-method="init"</code>配置。
 * <p/>
 * <pre>
 * &lt;!-- redis configuration --&gt;
 *     &lt;bean id=&quot;redisCacheMgr&quot; class=&quot;com.baidu.unbiz.redis.RedisCacheManager&quot;
 *     init-method=&quot;init&quot;&gt;
 *         &lt;property name=&quot;clientList&quot;&gt;
 *             &lt;list&gt;
 *                 &lt;ref bean=&quot;redisClient&quot; /&gt;
 *             &lt;/list&gt;
 *         &lt;/property&gt;
 *         &lt;property name=&quot;retryTimes&quot;&gt;
 *             &lt;value&gt;${redis.group1.retry.times}&lt;/value&gt;
 *         &lt;/property&gt;
 *         &lt;property name=&quot;evictorDelayCheckSeconds&quot;&gt;
 *             &lt;value&gt;${redis.evictor.delayCheckSeconds}&lt;/value&gt;
 *         &lt;/property&gt;
 *         &lt;property name=&quot;evictorCheckPeriodSeconds&quot;&gt;
 *             &lt;value&gt;${redis.evictor.checkPeriodSeconds}&lt;/value&gt;
 *         &lt;/property&gt;
 *         &lt;property name=&quot;evictorFailedTimesToBeTickOut&quot;&gt;
 *             &lt;value&gt;${redis.evictor.failedTimesToBeTickOut}&lt;/value&gt;
 *         &lt;/property&gt;
 *     &lt;/bean&gt;
 *
 *     &lt;bean id=&quot;redisClientConfig&quot; class=&quot;com.baidu.unbiz.redis.config.RedisHAClientConfig&quot;&gt;
 *         &lt;property name=&quot;cacheName&quot;&gt;
 *             &lt;value&gt;${redis.group1.client1.name}&lt;/value&gt;
 *         &lt;/property&gt;
 *         &lt;property name=&quot;redisServerHost&quot;&gt;
 *             &lt;value&gt;${redis.group1.client1.host}&lt;/value&gt;
 *         &lt;/property&gt;
 *         &lt;property name=&quot;redisServerPort&quot;&gt;
 *             &lt;value&gt;${redis.group1.client1.port}&lt;/value&gt;
 *         &lt;/property&gt;
 *         &lt;property name=&quot;timeout&quot;&gt;
 *             &lt;value&gt;${redis.group1.client1.timeout}&lt;/value&gt;
 *         &lt;/property&gt;
 *         &lt;property name=&quot;redisAuthKey&quot;&gt;
 *             &lt;value&gt;${redis.group1.client1.password}&lt;/value&gt;
 *         &lt;/property&gt;
 *     &lt;/bean&gt;
 *
 *     &lt;bean id=&quot;redisClient&quot; class=&quot;com.baidu.unbiz.redis.RedisClient&quot;&gt;
 *         &lt;constructor-arg&gt;
 *             &lt;ref bean=&quot;redisClientConfig&quot; /&gt;
 *         &lt;/constructor-arg&gt;
 *     &lt;/bean&gt;
 * </pre>
 *
 * @author Zhang Xu
 */
public class RedisCacheManager {

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * 该管理类中维护的client连接集合，每个client均是连接池化的
     */
    private List<RedisClient> clientList;

    /**
     * 重试是指当集群中所有的服务都暂时不可用时的retry times，默认为1
     */
    private int retryTimes = 1;

    /**
     * 失效检测器在RedisCacheManager初始化后的启动推迟运行时间
     */
    private int evictorDelayCheckSeconds = 300;

    /**
     * 失效检测器检查时间间隔，当大于0时表示启用检测器，否则不启动检测线程
     */
    private int evictorCheckPeriodSeconds = 30;

    /**
     * 失效检测器检查到最多容忍的调用失败次数
     */
    private int evictorFailedTimesToBeTickOut = 6;

    /**
     * 失效检测器
     */
    private Evictor evictor;

    /**
     * redis回调调用
     *
     * @param redisCallBack
     * @param clients
     * @param key
     * @param isRead
     *
     * @return
     */
    private <T> T execute(RedisCallBack<T> redisCallBack, List<RedisClient> clients, Object key, boolean isRead) {
        for (int i = 0; i < getRetryTimes(); i++) {
            boolean result = redisCallBack.doInRedis(clients, isRead, key);
            if (result) {
                return redisCallBack.getResult();
            }
        }

        Throwable e = redisCallBack.getException();
        if (e != null) {
            logger.error("Return null because exception occurs: " + e.getMessage(), e);
        }
        return null;
    }

    /**
     * 根据传入的client列表构造高可用的redis缓存管理器
     *
     * @param clients
     *
     * @return
     */
    public static RedisCacheManager of(List<RedisClient> clients) {
        RedisCacheManager redisCacheMgr = new RedisCacheManager();
        redisCacheMgr.setClientList(clients);
        return redisCacheMgr;
    }

    /**
     * 初始化方法
     */
    public void init() {
        startEvictor();
    }

    /**
     * 开启失效剔除/恢复策略监听线程
     */
    private void startEvictor() {
        cancelEvictor();
        if (evictorCheckPeriodSeconds > 0) {
            logger.info("Start redis client evictor...");
            evictor = new Evictor(getClientList(), evictorFailedTimesToBeTickOut);
            EvictionTimer.schedule(evictor, evictorDelayCheckSeconds, evictorCheckPeriodSeconds);
        }
    }

    /**
     * 关闭失效剔除/恢复策略监听线程
     */
    private void cancelEvictor() {
        if (null != evictor) {
            EvictionTimer.cancel(evictor);
            evictor = null;
        }
    }

    /**
     * 关闭管理器中维护的redis列表的连接池
     */
    public void shutdown() {
        if (clientList == null) {
            return;
        }
        cancelEvictor();
        for (RedisClient connection : clientList) {
            try {
                connection.shutdown();
            } catch (Exception e) {
                logger.debug(e.getMessage(), e);
            }
        }
    }

    public Object getSet(final Object key, final Integer expiration, final Object obj) {
        List<RedisClient> clients = this.getAliveClients(key);
        if (isAtLeastOneAvailable(clients)) {
            return this.execute(new BaseRedisCallBack<Object>() {
                public Object doOperation(RedisClient client) throws Exception {
                    return client.getSet(key.toString(), obj, expiration);
                }

                public String getOptionType() {
                    return "GETSET";
                }
            }, clients, key, false);
        }
        return null;
    }

    public String put(final Object key, final Object obj) {
        return this.put(key, -1, obj);
    }

    public String put(final Object key, final Integer expiration, final Object obj) {
        List<RedisClient> clients = this.getAliveClients(key);
        String cacheName = null;
        if (isAtLeastOneAvailable(clients)) {
            cacheName = clients.get(0).getCacheName();
            this.execute(new BaseRedisCallBack<Boolean>() {
                public Boolean doOperation(RedisClient client) throws Exception {
                    return client.set(key.toString(), obj, expiration);
                }

                public String getOptionType() {
                    return "PUT";
                }
            }, clients, key, false);
        }
        return cacheName;
    }

    public Object get(final Object key) {
        List<RedisClient> clients = this.getAliveClients(key);
        if (isAtLeastOneAvailable(clients)) {
            return this.execute(new BaseRedisCallBack<Object>() {
                public Object doOperation(RedisClient client) throws Exception {
                    return client.get(key.toString());
                }

                public String getOptionType() {
                    return "GET";
                }
            }, clients, key, true);
        }
        return null;
    }

    public String remove(final Object key) {
        List<RedisClient> clients = this.getAliveClients(key);
        String cacheName = null;
        if (isAtLeastOneAvailable(clients)) {
            cacheName = clients.get(0).getCacheName();
            this.execute(new BaseRedisCallBack<Boolean>() {
                public Boolean doOperation(RedisClient client) throws Exception {
                    return client.delete(key.toString());
                }

                public String getOptionType() {
                    return "REMOVE";
                }
            }, clients, key, false);
        }
        return cacheName;
    }

    public String replace(final Object key, final Object obj) {
        return this.replace(key, -1, obj);
    }

    public String replace(final Object key, final Integer expiration, final Object obj) {
        return put(key, expiration, obj);
    }

    public boolean existsKey(final String key) {
        List<RedisClient> clients = this.getAliveClients(key);
        if (isAtLeastOneAvailable(clients)) {
            return this.execute(new BaseRedisCallBack<Boolean>() {
                public Boolean doOperation(RedisClient client) throws Exception {
                    return client.exists(key);
                }

                public String getOptionType() {
                    return "EXIST";
                }
            }, clients, key, true);
        }
        return false;
    }

    public boolean extendTime(final String key, final Integer expirationMs) {
        List<RedisClient> clients = this.getAliveClients(key);
        if (isAtLeastOneAvailable(clients)) {

            return this.execute(new BaseRedisCallBack<Boolean>() {
                public Boolean doOperation(RedisClient client) throws Exception {
                    return client.expire(key, expirationMs / 1000);
                }

                public String getOptionType() {
                    return "EXPIRE";
                }
            }, clients, key, false);
        }
        return false;
    }

    public void hput(final String key, final String field, final Serializable fieldValue) {
        List<RedisClient> clients = this.getAliveClients(key);
        if (isAtLeastOneAvailable(clients)) {
            this.execute(new BaseRedisCallBack<Object>() {
                public Object doOperation(RedisClient client) throws Exception {
                    client.hput(key, field, fieldValue);
                    return null;
                }

                public String getOptionType() {
                    return "HPUT";
                }
            }, clients, key, false);
        }
    }

    public Object hget(final String key, final String field) {
        List<RedisClient> clients = this.getAliveClients(key);
        if (isAtLeastOneAvailable(clients)) {
            return this.execute(new BaseRedisCallBack<Object>() {
                public Object doOperation(RedisClient client) throws Exception {
                    return client.hget(key, field);
                }

                public String getOptionType() {
                    return "HGET";
                }
            }, clients, key, true);
        }
        return null;
    }

    public boolean hdel(final String key, final String field) {
        List<RedisClient> clients = this.getAliveClients(key);
        if (isAtLeastOneAvailable(clients)) {
            return this.execute(new BaseRedisCallBack<Boolean>() {
                public Boolean doOperation(RedisClient client) throws Exception {
                    return client.hdel(key, field);
                }

                public String getOptionType() {
                    return "HDEL";
                }
            }, clients, key, false);
        }
        return false;
    }

    public Set<String> hKeys(final String key) {
        List<RedisClient> clients = this.getAliveClients(key);
        if (isAtLeastOneAvailable(clients)) {
            return this.execute(new BaseRedisCallBack<Set<String>>() {
                public Set<String> doOperation(RedisClient client) throws Exception {
                    return client.hKeys(key);
                }

                public String getOptionType() {
                    return "HKEYS";
                }
            }, clients, key, true);
        }
        return Collections.emptySet();
    }

    public List<Object> hValues(final String key) {
        List<RedisClient> clients = this.getAliveClients(key);
        if (isAtLeastOneAvailable(clients)) {
            return this.execute(new BaseRedisCallBack<List<Object>>() {
                public List<Object> doOperation(RedisClient client) throws Exception {
                    return client.hValues(key);
                }

                public String getOptionType() {
                    return "HVALUES";
                }
            }, clients, key, true);
        }
        return Collections.emptyList();
    }

    public boolean hExists(final String key, final String field) {
        List<RedisClient> clients = this.getAliveClients(key);
        if (isAtLeastOneAvailable(clients)) {
            return this.execute(new BaseRedisCallBack<Boolean>() {
                public Boolean doOperation(RedisClient client) throws Exception {
                    return client.hExists(key, field);
                }

                public String getOptionType() {
                    return "HEXISTS";
                }
            }, clients, key, true);
        }
        return false;
    }

    public long hLen(final String key) {
        List<RedisClient> clients = this.getAliveClients(key);
        if (isAtLeastOneAvailable(clients)) {
            return this.execute(new BaseRedisCallBack<Long>() {
                public Long doOperation(RedisClient client) throws Exception {
                    return client.hLen(key);
                }

                public String getOptionType() {
                    return "HLEN";
                }
            }, clients, key, true);
        }
        return 0;
    }

    public void hmSet(final String key, final Map<String, Serializable> values) {
        List<RedisClient> clients = this.getAliveClients(key);
        if (isAtLeastOneAvailable(clients)) {
            this.execute(new BaseRedisCallBack<Object>() {
                public Object doOperation(RedisClient client) throws Exception {
                    client.hmSet(key, values);
                    return null;
                }

                public String getOptionType() {
                    return "HMSET";
                }
            }, clients, key, false);
        }
    }

    public List<Object> hmGet(final String key, final String... fields) {
        List<RedisClient> clients = this.getAliveClients(key);
        if (isAtLeastOneAvailable(clients)) {
            return this.execute(new BaseRedisCallBack<List<Object>>() {
                public List<Object> doOperation(RedisClient client) throws Exception {
                    return client.hmGet(key, fields);
                }

                public String getOptionType() {
                    return "HMGET";
                }
            }, clients, key, true);
        }
        return Collections.emptyList();
    }

    public List<String> hmGetByStringSerializer(final String key, final String... fields) {
        List<RedisClient> clients = this.getAliveClients(key);
        if (isAtLeastOneAvailable(clients)) {
            return this.execute(new BaseRedisCallBack<List<String>>() {
                public List<String> doOperation(RedisClient client) throws Exception {
                    return client.hmGetByStringSerializer(key, fields);
                }

                public String getOptionType() {
                    return "HMGET-STRING_SERIAL";
                }
            }, clients, key, true);
        }
        return Collections.emptyList();
    }

    public void hmSetByStringSerializer(final String key, final Map<String, String> values) {
        List<RedisClient> clients = this.getAliveClients(key);
        if (isAtLeastOneAvailable(clients)) {
            this.execute(new BaseRedisCallBack<Object>() {
                public Object doOperation(RedisClient client) throws Exception {
                    client.hmSetByStringSerializer(key, values);
                    return null;
                }

                public String getOptionType() {
                    return "HMSET-STRING_SERIAL";
                }
            }, clients, key, false);
        }
    }

    public boolean sAdd(final String key, final String member) throws Exception {
        List<RedisClient> clients = this.getAliveClients(key);
        if (isAtLeastOneAvailable(clients)) {
            this.execute(new BaseRedisCallBack<Object>() {
                public Object doOperation(RedisClient client) throws Exception {
                    return client.sAdd(key, member);
                }

                public String getOptionType() {
                    return "SADD";
                }
            }, clients, key, false);
        }
        return false;
    }

    public boolean sRem(final String key, final String member) throws Exception {
        List<RedisClient> clients = this.getAliveClients(key);
        if (isAtLeastOneAvailable(clients)) {
            this.execute(new BaseRedisCallBack<Object>() {
                public Object doOperation(RedisClient client) throws Exception {
                    client.sRem(key, member);
                    return null;
                }

                public String getOptionType() {
                    return "HMSET-STRING_SERIAL";
                }
            }, clients, key, false);
        }
        return false;
    }

    public Set<String> sMembers(final String key) throws Exception {
        List<RedisClient> clients = this.getAliveClients(key);
        if (isAtLeastOneAvailable(clients)) {
            return this.execute(new BaseRedisCallBack<Set<String>>() {
                public Set<String> doOperation(RedisClient client) throws Exception {
                    return client.sMembers(key);
                }

                public String getOptionType() {
                    return "SMEMBERS";
                }
            }, clients, key, true);
        }
        return Collections.emptySet();
    }

    public Map<String, Object> hGetAll(final String key) {
        List<RedisClient> clients = this.getAliveClients(key);
        if (isAtLeastOneAvailable(clients)) {
            return this.execute(new BaseRedisCallBack<Map<String, Object>>() {
                public Map<String, Object> doOperation(RedisClient client) throws Exception {
                    return client.hGetAll(key);
                }

                public String getOptionType() {
                    return "HGETALL";
                }
            }, clients, key, true);
        }
        return Collections.emptyMap();
    }

    public void lpush(final String key, final Object value) {
        List<RedisClient> clients = this.getAliveClients(key);
        if (isAtLeastOneAvailable(clients)) {
            this.execute(new BaseRedisCallBack<Object>() {
                public Object doOperation(RedisClient client) throws Exception {
                    client.lpush(key, value);
                    return null;
                }

                public String getOptionType() {
                    return "lpush";
                }
            }, clients, key, false);
        }
    }

    public void rpush(final String key, final Object value) {
        List<RedisClient> clients = this.getAliveClients(key);
        if (isAtLeastOneAvailable(clients)) {
            this.execute(new BaseRedisCallBack<Object>() {
                public Object doOperation(RedisClient client) throws Exception {
                    client.rpush(key, value);
                    return null;
                }

                public String getOptionType() {
                    return "rpush";
                }
            }, clients, key, false);
        }
    }

    public Object lpop(final String key, final Class<?> cls) {
        List<RedisClient> clients = this.getAliveClients(key);
        if (isAtLeastOneAvailable(clients)) {
            return this.execute(new BaseRedisCallBack<Object>() {
                public Object doOperation(RedisClient client) throws Exception {
                    return client.lpop(key, cls);
                }

                public String getOptionType() {
                    return "lpop";
                }
            }, clients, key, true);
        }
        return null;
    }

    public Object rpop(final String key, final Class<?> cls) {
        List<RedisClient> clients = this.getAliveClients(key);
        if (isAtLeastOneAvailable(clients)) {
            return this.execute(new BaseRedisCallBack<Object>() {
                public Object doOperation(RedisClient client) throws Exception {
                    return client.rpop(key, cls);
                }

                public String getOptionType() {
                    return "rpop";
                }
            }, clients, key, true);
        }
        return null;
    }

    public Long incr(final String key) {
        List<RedisClient> clients = this.getAliveClients(key);
        if (isAtLeastOneAvailable(clients)) {
            return this.execute(new BaseRedisCallBack<Long>() {
                public Long doOperation(RedisClient client) throws Exception {
                    return client.incr(key);
                }

                public String getOptionType() {
                    return "incr";
                }
            }, clients, key, true);
        }
        return 0L;
    }

    public Long incrBy(final String key, final long integer) {
        List<RedisClient> clients = this.getAliveClients(key);
        if (isAtLeastOneAvailable(clients)) {
            return this.execute(new BaseRedisCallBack<Long>() {
                public Long doOperation(RedisClient client) throws Exception {
                    return client.incrBy(key, integer);
                }

                public String getOptionType() {
                    return "INCRBY";
                }
            }, clients, key, true);
        }
        return 0L;
    }

    /**
     * Set key to hold string value if key does not exist. In that case, it is equal to SET.
     * When key already holds a value, no operation is performed. SETNX is short for "SET if N ot e X ists".
     *
     * @param key        Key to be operated.
     * @param expiration Expiration time
     * @param obj        Object to be set.
     *
     * @return 1 if the key was set, 0 if hte key was not set.
     *
     * @see <a href="http://redis.io/commands/setnx">Redis: SETNX</a>
     */
    public Long setnx(final Object key, final int expiration, final Object obj) {
        List<RedisClient> clients = this.getAliveClients(key);
        if (isAtLeastOneAvailable(clients)) {
            return this.execute(new BaseRedisCallBack<Long>() {
                public Long doOperation(RedisClient client) throws Exception {
                    return client.setnx(key.toString(), obj, expiration);
                }

                public String getOptionType() {
                    return "SETNX";
                }
            }, clients, key, false);
        }
        return 0L;
    }

    /**
     * 检查至少一个redis client可用
     *
     * @param clients
     *
     * @throws
     */
    private boolean isAtLeastOneAvailable(Collection<RedisClient> clients) throws RedisOperationException {
        if (clients.isEmpty()) {
            throw new RedisOperationException("All redis client is disconnected! Please check the basic availablity " +
                    "of redis!");
        }
        return true;
    }

    /**
     * 获取所有标记为可用的redis连接列表
     *
     * @param key
     *
     * @return
     */
    public List<RedisClient> getAliveClients(Object key) {
        List<RedisClient> aliveClients = new ArrayList<RedisClient>();
        for (RedisClient redisClient : clientList) {
            if (redisClient.isAlive()) {
                aliveClients.add(redisClient);
            }
        }
        return aliveClients;
    }

    private int getRetryTimes() {
        return retryTimes;
    }

    public void setRetryTimes(int retryTimes) {
        this.retryTimes = retryTimes;
    }

    public RedisCacheManager buildRetryTimes(int retryTimes) {
        this.retryTimes = retryTimes;
        return this;
    }

    public List<RedisClient> getClientList() {
        return clientList;
    }

    public void setClientList(List<RedisClient> clientList) {
        this.clientList = clientList;
    }

    public int getEvictorDelayCheckSeconds() {
        return evictorDelayCheckSeconds;
    }

    public void setEvictorDelayCheckSeconds(int evictorDelayCheckSeconds) {
        this.evictorDelayCheckSeconds = evictorDelayCheckSeconds;
    }

    public int getEvictorCheckPeriodSeconds() {
        return evictorCheckPeriodSeconds;
    }

    public void setEvictorCheckPeriodSeconds(int evictorCheckPeriodSeconds) {
        this.evictorCheckPeriodSeconds = evictorCheckPeriodSeconds;
    }

    public int getEvictorFailedTimesToBeTickOut() {
        return evictorFailedTimesToBeTickOut;
    }

    public void setEvictorFailedTimesToBeTickOut(int evictorFailedTimesToBeTickOut) {
        this.evictorFailedTimesToBeTickOut = evictorFailedTimesToBeTickOut;
    }

    public RedisCacheManager buildEvictorDelayCheckSeconds(int evictorDelayCheckSeconds) {
        this.evictorDelayCheckSeconds = evictorDelayCheckSeconds;
        return this;
    }

    public RedisCacheManager buildEvictorCheckPeriodSeconds(int evictorCheckPeriodSeconds) {
        this.evictorCheckPeriodSeconds = evictorCheckPeriodSeconds;
        return this;
    }

    public RedisCacheManager buildEvictorFailedTimesToBeTickOut(int evictorFailedTimesToBeTickOut) {
        this.evictorFailedTimesToBeTickOut = evictorFailedTimesToBeTickOut;
        return this;
    }

}
