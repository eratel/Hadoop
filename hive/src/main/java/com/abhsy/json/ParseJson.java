package com.abhsy.json;

import java.io.IOException;

import org.apache.hadoop.hive.ql.exec.UDF;

import parquet.org.codehaus.jackson.JsonParseException;
import parquet.org.codehaus.jackson.map.JsonMappingException;
import parquet.org.codehaus.jackson.map.ObjectMapper;


//{"movie":"1193","rate":"5","timeStamp":"978300760","uid":"1"}
public class ParseJson extends UDF {

    public String evaluate(String line) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            MovieRate bean = objectMapper.readValue(line, MovieRate.class);
            return bean.toString();
        } catch (Exception e) {
            e.printStackTrace();

        }
        return "";
    }
}