package com.abhsy.JUC;

import java.util.concurrent.CountDownLatch;

/**
 * @program: abhsy-hadoop
 * @author: jikai.sun
 * @create: 2018-08-31
 **/

/**
 *  CountDownLatch  可以理解为计数器
 */
public class TestCountDownLatch {

    public static void main(String[] args) {
        /**
         * 将CountDownLatch的计数器初始化为n
         */
        final CountDownLatch latch = new CountDownLatch(50);
        LatchDemo ld = new LatchDemo(latch);

        long start = System.currentTimeMillis();

        for (int i = 0; i < 50; i++) {
            new Thread(ld).start();
        }

        try {
            /**
             * 当计数器的值变为0时，在CountDownLatch上 await() 的线程就会被唤醒
             */
            latch.await();
        } catch (InterruptedException e) {
        }

        long end = System.currentTimeMillis();

        System.out.println("耗费时间为：" + (end - start));
    }

}

class LatchDemo implements Runnable {

    private CountDownLatch latch;

    public LatchDemo(CountDownLatch latch) {
        this.latch = latch;
    }

    @Override
    public void run() {
        synchronized (this) {
            try {
                for (int i = 0; i < 100; i++) {
                    if (i % 2 == 0) {
                        System.out.println(Thread.currentThread().getName() + "--->" + i);
                    }
                }
            } finally {
                /**
                 * 每当一个任务线程执行完毕，就将计数器减1
                 */
                latch.countDown();
            }
        }

    }
}
