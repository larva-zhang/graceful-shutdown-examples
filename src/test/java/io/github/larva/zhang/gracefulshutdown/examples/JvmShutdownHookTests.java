package io.github.larva.zhang.gracefulshutdown.examples;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import junit.framework.TestCase;
import org.junit.Assert;

/**
 * JvmShutdownHookTests
 *
 * @author larva-zhang
 * @date 2022/6/30
 * @since 1.0
 */
public class JvmShutdownHookTests extends TestCase {

    public void testJvmShutdownHookInvokeAfterCodeExecutionComplete() {
        AtomicInteger counter = new AtomicInteger(0);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            Assert.assertEquals(10, counter.get());
            System.out.println("ShutdownHook invoke after code execution complete");
        }));
        for (int i = 0; i < 10; i++) {
            counter.incrementAndGet();
        }
    }

    public void testJvmShutdownHookInvokeAfterCallSystemExit() {
        AtomicInteger counter = new AtomicInteger(0);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            Assert.assertEquals(5, counter.get());
            System.out.println("ShutdownHook invoke after call system exit");
        }));
        for (int i = 0; i < 10; i++) {
            int val = counter.incrementAndGet();
            if (val == 5) {
                System.exit(1);
            }
        }
    }

    @SuppressWarnings({"MismatchedQueryAndUpdateOfCollection", "InfiniteLoopStatement"})
    public void testJvmShutdownHookInvokeAfterOOM() {
        Runtime.getRuntime()
            .addShutdownHook(new Thread(() -> System.out.println("ShutdownHook invoke after OOM")));
        throw Assert.assertThrows(OutOfMemoryError.class, () -> {
            // generate OOM
            List<ByteBuffer> byteBufferList = new ArrayList<>();
            while (true) {
                byteBufferList.add(ByteBuffer.allocate(Integer.MAX_VALUE));
            }
        });
    }

    @SuppressWarnings("InfiniteLoopStatement")
    public void testJvmShutdownHookParallelInvoke() {
        // 模拟数据库连接池
        ExecutorService mockDatabaseConnectionPool = Executors.newFixedThreadPool(1);
        // 模拟业务线程池
        ExecutorService mockBusinessThreadPool = Executors.newFixedThreadPool(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            // hookA
            try {
                // 等待模拟数据库连接池先关闭
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.out.println("HookA等待时发生InterruptedException");
            }
            mockBusinessThreadPool.shutdown();
            System.out.println("ShutdownHook A invoke terminate mock business thread pool");
        }));
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            mockDatabaseConnectionPool.shutdown();
            try {
                boolean terminatedBeforeTimeout = mockDatabaseConnectionPool.awaitTermination(1, TimeUnit.SECONDS);
                if (terminatedBeforeTimeout) {
                    // 超时前所有任务已结束
                    System.out.println("模拟的数据库连接池在超时前已关闭");
                } else {
                    // 发生超时，但因为是异步执行，不能确定线程池一定还有任务未结束
                    if (mockDatabaseConnectionPool.isTerminated()) {
                        System.out.println("模拟的数据库连接池在等待超时后仍然已关闭");
                    } else {
                        System.out.println("模拟的数据库连接池在等待超时后仍然未关闭");
                    }
                }
            } catch (InterruptedException e) {
                System.out.println("模拟的数据库连接池在等待关闭过程中发生了InterruptedException");
            }
            System.out.println("ShutdownHook B invoke terminate mock db connection pool");
        }));
        mockBusinessThreadPool.submit(()->{
            while (true) {
                // 模拟业务线程调用数据库查询
                Boolean mockDbExecuteResult = null;
                try {
                    Future<Boolean> future = mockDatabaseConnectionPool.submit(
                        () -> {
                            try {
                                // 模拟执行数据库查询
                                Thread.sleep(100);
                                return Boolean.TRUE;
                            } catch (InterruptedException e) {
                                System.out.println("模拟的数据库连接池捕获InterruptedException");
                                Thread.currentThread().interrupt();
                                return Boolean.FALSE;
                            }
                        });
                    mockDbExecuteResult = future.get(300, TimeUnit.MILLISECONDS);
                } catch (Exception e) {
                    System.out.println("模拟数据库查询发生异常："+e.getMessage());
                }
                Assert.assertEquals(Boolean.TRUE, mockDbExecuteResult);
            }
        });
        try {
            // 主线程等待3秒，让模拟线程池都工作起来
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.out.println("主线程捕获InterruptedException");
        }
    }


}
