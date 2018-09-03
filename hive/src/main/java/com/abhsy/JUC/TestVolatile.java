package com.abhsy.JUC;

/**
 * @program: abhsy-hadoop
 * @author: jikai.sun
 * @create: 2018-08-31
 **/
public class TestVolatile {

    public static void main(String[] args) {
        ThreadDemo td = new ThreadDemo();
        new Thread(td).start();

        while (true) {
            //如果不使用volatile关键字，无法或者另外一条线程中的变化的值，false=true
            if (td.isFlag()) {
                System.out.println("########");
                break;
            }
        }
    }
}

class ThreadDemo implements Runnable {
    private volatile boolean flag = false;

    public void run() {
        try {
            // 该线程 sleep(200), 导致了程序无法执行成功
            Thread.sleep(200);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        flag = true;

        System.out.println("flag=" + isFlag());
    }

    public boolean isFlag() {
        return flag;
    }

    public void setFlag(boolean flag) {
        this.flag = flag;
    }
}
