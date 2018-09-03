package com.abhsy.JUC;

import java.util.concurrent.*;

/**
 * @program: abhsy-hadoop
 * @author: jikai.sun
 * @create: 2018-08-31
 **/

/**
 * Callable<Integer> 实现 Callable 接口。
 * 相较于实现 Runnable 接口的方式，Callable 方法可以有返回值，并且可以抛出异常。
 * 执行 Callable 方式，需要 FutureTask 实现类的支持，用于接收运算结果。FutureTask 是 Future 接口的实现类。
 */
public class TestCallable {

    public static void main(String[] args) {
        ThreadDemo td = new ThreadDemo();

        //1.执行 Callable 方式，需要 FutureTask 实现类的支持，用于接收运算结果。
        FutureTask<Integer> result = new FutureTask<>(td);
        new Thread(result).start();
        /**
         * executor.submit()可以执行 callable 和 Runnable
         */
//        ExecutorService executor = Executors.newFixedThreadPool(3);
//        executor.submit()
        //2.接收线程运算后的结果
        try {
            Integer sum = result.get();  //线程在运行的时候，FutureTask 的get方法并没有执行，而是在等待线程运行的结果。FutureTask 可用于 闭锁
            System.out.println(sum);
            System.out.println("------------------------------------");
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

    }


    static class ThreadDemo implements Callable<Integer> {

        @Override
        public Integer call() throws Exception {
            int sum = 0;

            for (int i = 0; i <= 100000; i++) {
                sum += i;
            }

            return sum;
        }
    }
}
