package com.abhsy.flowsum;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @program: abhsy-hadoop
 * @author: jikai.sun
 * @create: 2018-08-08
 **/

/**
 * 实现序列化
 * implements  Writable
 */
public class FlowBean implements WritableComparable<FlowBean> {

    private long upflow;

    private long downflow;

    private long sumflow;

    //排序

    /**
     * > 正数
     * < 负数
     *  =  0
     */
    @Override
    public int compareTo(FlowBean o) {
        //倒序
        return (int) (o.getSumflow() - this.getSumflow());
    }

    //序列化
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(upflow);
        out.writeLong(downflow);
        out.writeLong(sumflow);
    }

    //反序列化
    //反序列化顺序跟序列化顺序保持一致
    @Override
    public void readFields(DataInput in) throws IOException {
        this.upflow = in.readLong();
        this.downflow = in.readLong();
        this.sumflow = in.readLong();
    }

    //序列化反序列化会调用无参构造
    public FlowBean() {
    }

    public void set(long upFlow, long downFlow) {
        this.upflow = upFlow;
        this.downflow = downFlow;
        this.sumflow = upFlow+downFlow;
    }

    public FlowBean(long upflow, long downflow, long sumflow) {

        this.upflow = upflow;
        this.downflow = downflow;
        this.sumflow = sumflow;
    }

    public void setUpflow(long upflow) {

        this.upflow = upflow;
    }

    public void setDownflow(long downflow) {
        this.downflow = downflow;
    }

    public void setSumflow(long sumflow) {
        this.sumflow = sumflow;
    }

    public long getUpflow() {

        return upflow;
    }

    public long getDownflow() {
        return downflow;
    }

    public long getSumflow() {
        return sumflow;
    }

    @Override
    public String toString() {
        return "FlowBean{" +
                "upflow=" + upflow +
                ", downflow=" + downflow +
                ", sumflow=" + sumflow +
                '}';
    }


}
