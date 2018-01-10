/*
 * Copyright (C), 2014-2017, 江苏乐博国际投资发展有限公司
 * FileName: WordCountMapper.java
 * Author:   zhangdanji
 * Date:     2017年05月28日
 * Description: 测试Mapper  
 */
package com.mychebao.hadoop.mapper;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 测试Mapper
 *
 * @author zhangdanji
 */
public class WordCountMapper extends Mapper<LongWritable,Text,Text,LongWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = StringUtils.split(line," ");
        for(String word : words){
            context.write(new Text(word),new LongWritable(1));
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
