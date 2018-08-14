package com.abhsy.ordertopn;

import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @program: abhsy-hadoop
 * @author: jikai.sun
 * @create: 2018-08-14
 **/
public class ItemIdPartitioner extends Partitioner {

    @Override
    public int getPartition(Object key, Object value, int numPartitions) {
        OrderBean key1 = (OrderBean) key;
        switch (key1.getId()){
            case "pd001":
                return 0;
            case "pd005":
                return 1;
        }
        return  2;
    }
}
