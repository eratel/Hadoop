package com.abhsy.ordertopn;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @program: abhsy-hadoop
 * @author: jikai.sun
 * @create: 2018-08-14
 **/
public class OrderBean implements WritableComparable<OrderBean> {
    private String orderNo;
    private Double money;
    private String id;

    @Override
    public int compareTo(OrderBean o) {
        return (int) (o.getMoney() - this.getMoney());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(orderNo);
        out.writeDouble(money);
        out.writeUTF(id);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.orderNo = in.readUTF();
        this.money = in.readDouble();
        this.id = in.readUTF();
    }

    @Override
    public String toString() {
        return "OrderBean{" +
                "orderNo='" + orderNo + '\'' +
                ", money=" + money +
                ", id='" + id + '\'' +
                '}';
    }

    public void setOrderNo(String orderNo) {
        this.orderNo = orderNo;
    }

    public void setMoney(Double money) {
        this.money = money;
    }

    public String getOrderNo() {

        return orderNo;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getId() {

        return id;
    }

    public Double getMoney() {
        return money;
    }

}
