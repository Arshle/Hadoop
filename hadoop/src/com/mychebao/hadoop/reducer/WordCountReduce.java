/*
 * Copyright (C), 2014-2017, 江苏乐博国际投资发展有限公司
 * FileName: WordCountReduce.java
 * Author:   zhangdanji
 * Date:     2017年05月28日
 * Description: 测试Reduce  
 */
package com.mychebao.hadoop.reducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

/**
 * 测试Reduce
 *
 * @author zhangdanji
 */
public class WordCountReduce extends Reducer<Text,LongWritable,Text,LongWritable> {
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long count = 0;
        for(LongWritable value : values){
            count += value.get();
        }
        context.write(key,new LongWritable(count));
    }
}
