package com.abhsy.ordertopn;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @program: abhsy-hadoop
 * @author: jikai.sun
 * @create: 2018-08-14
 **/
public class GroupingComparator extends WritableComparator {

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean abean = (OrderBean) a;
        OrderBean bbean = (OrderBean) b;

        return abean.getId().compareTo(bbean.getId());
    }
}
