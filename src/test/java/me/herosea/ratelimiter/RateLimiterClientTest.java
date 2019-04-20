package me.herosea.ratelimiter;

import lombok.extern.slf4j.Slf4j;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.JedisPool;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Slf4j
public class RateLimiterClientTest {

    protected RateLimiterClient client;
    protected RateLimiterClient noSleepClient;
    JedisPool jedisPool = new JedisPool("127.0.0.1", 6379);

    @Before
    public void setUp() throws Exception {
        String originKey = "rateLimit-test";
        double rate = 100;
        int maxPermits = 100;
        client = new RateLimiterClient(jedisPool, originKey, rate, maxPermits);
        noSleepClient = new RateLimiterClient(jedisPool, originKey, rate, maxPermits, RateLimiterClient.Judge.createDefault(), new RateLimiterClient.SleepingStopwatch() {
            @Override
            protected void sleepAndWatch(long resultMill) {
                //do not sleep
            }
        });
    }

    @Test
    public void testGetRedisValues() {
        client.setRate(4, 5);
        log.info("redisValues: " + client.getRedisValues());
        Map<String, String> map = client.getRedisValues();
        assertEquals(4d, Double.parseDouble(map.get("rate")), 0.01);
        assertEquals(5, Integer.parseInt(map.get("max_permits")));
    }

    @Test
    public void acquire() {
        client.setRate(2, 2);

        assertTrue(client.tryAcquire());
        assertTrue(client.tryAcquire());
        assertFalse(client.tryAcquire());
        assertFalse(client.tryAcquire());
        IntStream.range(0, 5).forEach(i -> {
            try {
                Thread.sleep(501);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            assertTrue(client.tryAcquire());
        });

        client.delete();
        assertTrue(client.tryAcquire());
        assertFalse(client.tryAcquire(1, 20, 0));
    }

    @Test(timeout = 10000)
    public void performanceTest() {
        client.setRate(100, 100);
        long start = System.currentTimeMillis();
        IntStream.range(0, 1000).forEach(i -> client.tryAcquire());
        log.info("1000 次耗时:" + (System.currentTimeMillis() - start));
    }

    @Test
    public void performanceFusing() throws InterruptedException {
        client.setRate(30, 30);
        List<Boolean> ret = new ArrayList<>();
        long start = System.currentTimeMillis();
        for (int i = 0; i < 1100; i++) {
            ret.add(client.tryAcquire());
            long end = System.currentTimeMillis();
            long spend = end - start;
            if (spend > 1000) {
                long count = ret.stream().filter(t -> t).count();
                log.info("一秒钟获取token：" + count);
                assertTrue(count >= 29);
                assertTrue(count <= 62);
                start = end;
                ret.clear();
            }
            Thread.sleep(4);
        }
    }

    @Test
    public void acquireTokenWithAdvance() {
        client.setRate(10, 10);

        IntStream.range(0, 10).forEach(i -> assertTrue(client.tryAcquire()));

        long fusingCount = IntStream.range(0, 10)
                .mapToObj(i -> client.tryAcquire())
                .filter(t -> !t)
                .count();
        log.info("fusing size: " + fusingCount);
        assertTrue(fusingCount > 0 && fusingCount <= 10);

        //向前预支一秒
        IntStream.range(0, 10).forEach(i -> assertTrue(noSleepClient.tryAcquire(1, 0, 1000)));
        IntStream.range(0, 10).forEach(i -> assertFalse(noSleepClient.tryAcquire()));
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        long normalCount = IntStream.range(0, 10).mapToObj(i -> client.tryAcquire())
                .filter(t -> !t)
                .count();
        log.info("fusing size: " + normalCount);
        assertTrue(normalCount > 0 && normalCount <= 10);
    }

    @Test
    public void priorityTest() throws InterruptedException {
        int rate = 10;
        int maxPermits = rate;
        client = new RateLimiterClient(jedisPool, "limit-priority", rate, maxPermits);
        int threadCount = 3;
        CountDownLatch latch = new CountDownLatch(threadCount);
        ExecutorService service = Executors.newFixedThreadPool(threadCount);
        int times = 100;
        int sleepMill = 120;
        AcquireTask highPriority = new AcquireTask(client, latch, 0, sleepMill, times);
        AcquireTask middlePriority = new AcquireTask(client, latch, 20, sleepMill, times);
        AcquireTask lowPriority = new AcquireTask(client, latch, 50, sleepMill, times);
        service.submit(highPriority);
        service.submit(middlePriority);
        service.submit(lowPriority);
        latch.await();

        List<Boolean> highResult = highPriority.getResult();
        List<Boolean> middleResult = middlePriority.getResult();
        List<Boolean> lowResult = lowPriority.getResult();


        log.info("highResult:" + highResult);
        log.info("middleResult:" + middleResult);
        log.info("lowResult:" + lowResult);

        long count = highPriority.passCount() + middlePriority.passCount() + lowPriority.passCount();
        log.info("总通过量:" + count);
        assertTrue(count > times * sleepMill * rate / 1000);

        assertTrue(highPriority.passCount() == 100);
        assertTrue(highPriority.resultGroupCount().size() < middlePriority.resultGroupCount().size());
        assertTrue(middlePriority.resultGroupCount().size() > lowPriority.resultGroupCount().size());

        assertTrue(middleResult.get(0));
        assertTrue(middlePriority.passCount() > lowPriority.passCount());

        assertTrue(lowResult.get(0));
        assertTrue(lowPriority.passCount() < 5);

    }

    class AcquireTask implements Runnable {
        private RateLimiterClient client;
        private CountDownLatch latch;
        private long reservedPercent;
        private long sleepMill;
        private List<Boolean> result;
        private int times;

        public AcquireTask(RateLimiterClient client, CountDownLatch latch, long reservedPercent, long sleepMill, int times) {
            this.client = client;
            this.latch = latch;
            this.reservedPercent = reservedPercent;
            this.sleepMill = sleepMill;
            this.times = times;
            result = new ArrayList<>(times);
        }

        public List<Boolean> getResult() {
            return result;
        }

        public long passCount() {
            long count = result.stream().filter(t -> t).count();
            return count;
        }

        public List<Integer> resultGroupCount() {
            List<Integer> ret = new ArrayList<>();
            boolean current = true;
            int count = 0;
            int length = result.size();
            for (int i = 0; i < length; i++) {
                boolean r = result.get(i);
                if (current == r) {
                    count = count + 1;
                } else {
                    ret.add(count);
                    current = r;
                    count = 1;
                }
            }
            ret.add(count);
            return ret;
        }

        @Override
        public void run() {
            for (int i = 0; i < times; i++) {
                long start = System.currentTimeMillis();
                result.add(client.tryAcquire(1, reservedPercent, 0));
                long end = System.currentTimeMillis();
                long sleepTime = sleepMill + start - end;
                if (sleepTime < 0) {
                    log.warn("预设的等待时间比获取token的时间还短");
                    continue;
                }
                try {
                    Thread.sleep(sleepTime);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            latch.countDown();
        }
    }
}