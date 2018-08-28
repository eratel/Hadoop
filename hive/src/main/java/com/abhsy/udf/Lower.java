package com.abhsy.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @program: abhsy-hadoop
 * @author: jikai.sun
 * @create: 2018-08-28
 **/

/**
 * 自定义函数UDF
 *  add jar /path/hive-sample.jar;
 *  create temporary function lower as 'com.abhsy.udf.Lower';
 */
public class Lower extends UDF {

    public String evaluate(String s){
        if(null != s){
            return s.toLowerCase();
        }
        return null;
    }
}
