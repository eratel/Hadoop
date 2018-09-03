package com.abhsy.JUC;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @program: abhsy-hadoop
 * @author: jikai.sun
 * @create: 2018-08-31
 **/

/**
 * CopyOnWrite容器即写时复制的容器。通俗的理解是当我们往一个容器添加元素的时候，不直接往当前容器添加，
 * 而是先将当前容器进行Copy，复制出一个新的容器，然后新的容器里添加元素，添加完元素之后，再将原容器的引用指向新的容器。
 * 这样做的好处是我们可以对CopyOnWrite容器进行并发的读，而不需要加锁，因为当前容器不会添加任何元素。
 * 所以CopyOnWrite容器也是一种读写分离的思想，读和写不同的容器。
  CopyOnWriteArrayList的实现原理
 */
public class TestCopyOnWriteArrayList {

    public static void main(String[] args) {
        HelloThread ht = new HelloThread();

        for (int i = 0; i < 10; i++) {
            new Thread(ht).start();
        }
    }

}

class HelloThread implements Runnable {

//  private static List<String> list = Collections.synchronizedList(new ArrayList<String>());

        private static CopyOnWriteArrayList<String> list = new CopyOnWriteArrayList<>();
//    private static List list = new ArrayList<>();

    static {
        list.add("AA");
        list.add("BB");
        list.add("CC");
    }

    @Override
    public void run() {

        Iterator<String> it = list.iterator();

        while (it.hasNext()) {
            System.out.println(it.next());

            list.remove("AA");
            //普通的list会发生ConcurrentModificationException异常
        }

    }

}
