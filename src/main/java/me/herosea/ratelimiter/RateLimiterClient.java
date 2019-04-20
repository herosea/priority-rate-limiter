package me.herosea.ratelimiter;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/**
 * @author 周雄海
 */
@Slf4j
public class RateLimiterClient {

    protected final JedisPool jedisPool;
    protected final SleepingStopwatch sleepingStopwatch;
    protected final Judge judge;
    protected final String key;

    protected String sha1;

    protected double rate;
    protected int maxPermits;
    private static final int fullPercent = 100;

    public RateLimiterClient(JedisPool jedisPool, String originKey, double rate, int maxPermits) {
        this(jedisPool, originKey, rate, maxPermits, Judge.createDefault(), SleepingStopwatch.createFromSystemTimer());
    }

    public RateLimiterClient(JedisPool jedisPool, String originKey, double rate, int maxPermits, Judge judge, SleepingStopwatch sleepingStopwatch) {
        this.jedisPool = jedisPool;
        this.sleepingStopwatch = sleepingStopwatch;
        this.judge = judge;
        key = this.getKey(originKey);
        loadScriptFromClasspath("rate_limiter.lua");
        setRate(rate, maxPermits);
    }

    private void loadScriptFromClasspath(String scriptFile) {
        InputStream resourceAsStream = this.getClass().getClassLoader().getResourceAsStream(scriptFile);
        String rateLimiterClientLua = null;
        try {
            rateLimiterClientLua = IOUtils.toString(resourceAsStream, "UTF-8");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        try (Jedis jedis = jedisPool.getResource()) {
            sha1 = jedis.scriptLoad(rateLimiterClientLua);
        }
    }

    public void setRate(double rate, int maxPermits) {
        evalsha(key,
                RateLimiterConstants.RATE_LIMITER_INIT_METHOD,
                String.valueOf(maxPermits),
                String.valueOf(rate));

        this.rate = rate;
        this.maxPermits = maxPermits;
    }

    public Map<String, String> getRedisValues() {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.hgetAll(key);
        }
    }

    /**
     * 删除令牌桶
     */
    public void delete() {
        evalsha(key, RateLimiterConstants.RATE_LIMITER_DELETE_METHOD);
    }

    /**
     * 尝试获取token
     * 默认的permits为1
     *
     * @return
     */
    public boolean tryAcquire() {
        return tryAcquire(1);
    }

    /**
     * 尝试获取token
     * 默认等待0毫秒
     *
     * @param permits 获取的token数目
     * @return
     */
    public boolean tryAcquire(int permits) {
        return tryAcquire(permits, 0);
    }

    public boolean tryAcquire(int permits, long reservedPercent) {
        return tryAcquire(permits, reservedPercent, 0);
    }

    /**
     * 尝试获取token
     *
     * @param permits     获取的token数目
     * @param maxWaitMill 最长等待毫秒数
     * @return
     */
    public boolean tryAcquire(int permits, long reservedPercent, long maxWaitMill) {
        if (reservedPercent > 0) {
            return withRetryAcquire(permits, reservedPercent, maxWaitMill, 3);
        } else {
            return doTryAcquire(permits, reservedPercent, maxWaitMill);
        }
    }

    private boolean withRetryAcquire(int permits, long reservedPercent, long maxWaitMill, int retryTime) {
        boolean ret = doTryAcquire(permits, reservedPercent, 0);
        if (ret) {
            return true;
        } else if (retryTime <= 0) {
            return false;
        } else if (maxWaitMill <= 0) {
            return false;
        } else {
            long sleepTime = maxWaitMill / retryTime;
            long restMaxWaiteMill = maxWaitMill - sleepTime;
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return withRetryAcquire(permits, reservedPercent, restMaxWaiteMill, retryTime - 1);
        }
    }

    private boolean doTryAcquire(int permits, long reservedPercent, long maxWaitMillToRedis) {
        long waitMill = execute(permits, reservedPercent, maxWaitMillToRedis);
        boolean success = judge.judge(permits, reservedPercent, waitMill);
        if (success) {
            sleepingStopwatch.sleepAndWatch(waitMill);
        }
        return success;
    }

    protected long execute(int permits, long reservedPercent, long maxWaitMill) {
        try {
            if (permits < 1) {
                throw new RuntimeException("至少取一个令牌");
            }
            if (maxWaitMill < 0) {
                throw new RuntimeException("等待时间需要 >=0 ms");
            }
            if (reservedPercent < 0 || reservedPercent >= fullPercent) {
                throw new RuntimeException("保留百分比需要在1～100之间");
            }
            long millSecond = System.currentTimeMillis();
            Long acquire = (Long) evalsha(key,
                    RateLimiterConstants.RATE_LIMITER_ACQUIRE_METHOD,
                    String.valueOf(permits),
                    Long.toString(millSecond),
                    String.valueOf(reservedPercent),
                    String.valueOf(maxWaitMill)
            );

            if (acquire >= 0) {
                //pass
            } else if (acquire == -1) {
                //fusing
            } else {
                log.error("no rate limit config for key={}", key);
                //no config
            }
            return acquire;
        } catch (Throwable e) {
            log.error("get rate limit token from redis error,key=" + key, e);
            //fail
            return RateLimiterConstants.RESULT_FAIL;
        }
    }

    private Object evalsha(String... params) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.evalsha(sha1, 1, params);
        }
    }

    private String getKey(String key) {
        return RateLimiterConstants.RATE_LIMITER_KEY_PREFIX + key;
    }

    abstract static class Judge {

        protected abstract boolean judge(int permits, long reservedPercent, long resultMill);

        public static Judge createDefault() {
            return new Judge() {
                @Override
                protected boolean judge(int permits, long reservedPercent, long resultMill) {
                    if (resultMill >= 0) {
                        //token获取成功
                        return true;
                    } else if (resultMill == RateLimiterConstants.RESULT_FUSING) {
                        //token获取不成功
                        log.info("被限流,lua脚本断定.");
                        return false;
                    } else if (reservedPercent == 0) {
                        //异常情况。reservedPercent == 0 认为优先级比较高，允许通过
                        log.warn("不限流,配置缺失或异常.");
                        return true;
                    } else {
                        log.warn("被限流,配置缺失或异常.");
                        return false;
                    }
                }
            };
        }
    }

    abstract static class SleepingStopwatch {

        protected abstract void sleepAndWatch(long resultMill);

        public static SleepingStopwatch createFromSystemTimer() {
            return new SleepingStopwatch() {
                @Override
                protected void sleepAndWatch(long resultMill) {
                    if (resultMill > 0) {
                        //token获取成功，需等待
                        try {
                            log.info("整流启动，sleep:{}ms", resultMill);
                            Thread.sleep(resultMill);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            };
        }
    }
}
