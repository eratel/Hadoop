package com.abhsy.flowsum;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;

/**
 * @program: abhsy-hadoop
 * @author: jikai.sun
 * @create: 2018-08-09
 **/
public class ProviedPariti extends Partitioner<Text,FlowBean> {

    private static HashMap<String,Integer> map = new HashMap<>();
    static {
        map.put("135",0);
        map.put("136",1);
        map.put("137",2);
        map.put("138",3);
        map.put("139",4);
        map.put("140",5);
    }

    /**
     * 在每次reduce时会根据ket进行分区，找到对应reduceTask做Reduce操作
     * 默认为：KEY的HASHCODE % reduceTask数量
     * 分区规则
     */
    @Override
    public int getPartition(Text key, FlowBean flowBean, int numPartitions) {
        Integer code = map.get(key.toString().substring(0, 3));
        if(code != null){
            return code;
        }
        return 5;
    }
}









