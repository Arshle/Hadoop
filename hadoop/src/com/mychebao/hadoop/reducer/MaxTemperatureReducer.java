/*
 * Copyright (C), 2014-2017, 江苏乐博国际投资发展有限公司
 * FileName: MaxTemperatureReducer.java
 * Author:   zhangdanji
 * Date:     2017年07月26日
 * Description: 最高气温reducer  
 */
package com.mychebao.hadoop.reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 最高气温reducer
 *
 * @author zhangdanji
 */
public class MaxTemperatureReducer extends Reducer<Text,Text,Text,Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Integer total = 0;
        Integer maxYear = 0;
        Integer num = 0;
        for(Text value : values){
            String[] datas = value.toString().split(" ");
            Integer max = Integer.parseInt(datas[1]);
            if(Integer.parseInt(datas[0]) > maxYear){
                maxYear = Integer.parseInt(datas[0]);
            }
            total += max;
            num += 1;
        }
        context.write(new Text("year:" + maxYear.toString()),new Text("ava:" + total/num));
    }
}
