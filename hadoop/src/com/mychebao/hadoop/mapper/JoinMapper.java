/*
 * Copyright (C), 2014-2017, 江苏乐博国际投资发展有限公司
 * FileName: JoinMapper.java
 * Author:   zhangdanji
 * Date:     2017年08月09日
 * Description: 连接Mapper  
 */
package com.mychebao.hadoop.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * 连接Mapper
 *
 * @author zhangdanji
 */
public class JoinMapper extends Mapper<LongWritable,Text,Text,Text> {

    private final static String LEFT_FILENAME = "student_info.txt";
    private final static String RIGHT_FILENAME = "student_class_info.txt";
    private final static String LEFT_FLAG = "l";
    private final static String RIGHT_FLAG = "r";

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String filePath = ((FileSplit)context.getInputSplit()).getPath().toString();
        String fileFlag = null;
        String joinKey = null;
        String joinValue = null;
        if(LEFT_FILENAME.equals(filePath)){
            fileFlag = LEFT_FLAG;
            joinKey = value.toString().split("\t")[1];
            joinValue = value.toString().split("\t")[0];
        }else if(RIGHT_FILENAME.equals(filePath)){
            fileFlag = RIGHT_FLAG;
            joinKey = value.toString().split("\t")[0];
            joinValue = value.toString().split("\t")[1];
        }

        context.write(new Text(joinKey),new Text(joinValue + "\t" + fileFlag));
    }
}
