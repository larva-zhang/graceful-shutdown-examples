package io.github.larva.zhang.gracefulshutdown.examples;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import junit.framework.TestCase;

/**
 * ExecutorGracefulShutdownTests
 *
 * @author larva-zhang
 * @date 2022/7/3
 * @since 1.0
 */
public class ExecutorGracefulShutdownTests extends TestCase {

    public void testExecutorShutdown() {
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        for (int i = 0; i < 3; i++) {
            executorService.submit(() -> {
                try {
                    Thread.sleep(300);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        executorService.shutdown();
        int awaitCount = 0;
        boolean continueAwait;
        do {
            if (executorService.isTerminated()) {
                System.out.println("线程池已关闭，等待次数" + awaitCount);
                continueAwait = false;
            } else {
                awaitCount++;
                System.out.println("线程池未关闭，继续等待，等待次数" + awaitCount);
                try {
                    boolean terminatedBeforeTimeout = executorService.awaitTermination(300,
                        TimeUnit.MILLISECONDS);
                    if (terminatedBeforeTimeout) {
                        System.out.println("线程池在等待超时前关闭，等待次数" + awaitCount);
                        continueAwait = false;
                    } else {
                        // 虽然等待超时了，但因为是异步执行的原因，不能确定线程池一定还有任务未结束
                        if (executorService.isTerminated()) {
                            System.out.println("线程池在等待超时后仍正常关闭，等待次数" + awaitCount);
                            continueAwait = false;
                        } else {
                            System.out.println("线程池在终止时超过了最大等待时间，并且直到现在仍未全部执行完成，等待次数" + awaitCount);
                            continueAwait = true;
                        }
                    }
                } catch (InterruptedException e) {
                    System.out.println("线程池在等待关闭时发生InterruptedException，等待次数" + awaitCount);
                    continueAwait = true;
                }
            }
        } while (continueAwait);
    }

    public void testExecutorShutdownNow() {
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        for (int i = 0; i < 3; i++) {
            executorService.submit(() -> {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    System.out.println("线程池执行Task时发生InterruptedException");
                }
            });
        }
        List<Runnable> runnableList = executorService.shutdownNow();
        System.out.println("线程池执行shutdownNow，终止了等待任务" + runnableList.size() + "个");
        int awaitCount = 0;
        boolean continueAwait;
        do {
            if (executorService.isTerminated()) {
                System.out.println("线程池已关闭，等待次数" + awaitCount);
                continueAwait = false;
            } else {
                awaitCount++;
                System.out.println("线程池未关闭，继续等待，等待次数" + awaitCount);
                try {
                    boolean terminatedBeforeTimeout = executorService.awaitTermination(100,
                        TimeUnit.MILLISECONDS);
                    if (terminatedBeforeTimeout) {
                        System.out.println("线程池在等待超时前关闭，等待次数" + awaitCount);
                        continueAwait = false;
                    } else {
                        // 虽然等待超时了，但因为是异步执行的原因，不能确定线程池一定还有任务未结束
                        if (executorService.isTerminated()) {
                            System.out.println("线程池在等待超时后仍正常关闭，等待次数" + awaitCount);
                            continueAwait = false;
                        } else {
                            System.out.println("线程池在终止时超过了最大等待时间，并且直到现在仍未全部执行完成，等待次数" + awaitCount);
                            continueAwait = true;
                        }
                    }
                } catch (InterruptedException e) {
                    System.out.println("线程池在等待关闭时发生InterruptedException，等待次数" + awaitCount);
                    continueAwait = false;
                }
            }
        } while (continueAwait);
    }
}
