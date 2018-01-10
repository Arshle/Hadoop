/*
 * Copyright (C), 2014-2017, 江苏乐博国际投资发展有限公司
 * FileName: MaxTemperatureMapper.java
 * Author:   zhangdanji
 * Date:     2017年07月26日
 * Description: 最高气温mapper  
 */
package com.mychebao.hadoop.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

/**
 * 最高气温mapper
 *
 * @author zhangdanji
 */
public class MaxTemperatureMapper extends Mapper<LongWritable,Text,Text,Text>{

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] datas = line.split(" ");
        String year = datas[1];
        String month = datas[2];
        String day = datas[3];
        String max = datas[4];
        if("01".equals(month) && "01".equals(day)){
            context.write(new Text("data"),new Text(year + " " +max));
        }
    }
}
