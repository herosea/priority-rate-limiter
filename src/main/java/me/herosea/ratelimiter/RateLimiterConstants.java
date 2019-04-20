package me.herosea.ratelimiter;

/**
 * @author 周雄海
 */
public class RateLimiterConstants {
    public static final String RATE_LIMITER_KEY_PREFIX = "rate_limiter:";


    public static final String RATE_LIMITER_INIT_METHOD = "init";
    public static final String RATE_LIMITER_DELETE_METHOD = "delete";
    public static final String RATE_LIMITER_ACQUIRE_METHOD = "acquire";

    public static final int RESULT_FAIL = -3;
    public static final int RESULT_NO_CONFIG = -2;
    public static final int RESULT_FUSING = -1;
}
