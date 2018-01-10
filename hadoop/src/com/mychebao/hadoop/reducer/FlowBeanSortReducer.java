/*
 * Copyright (C), 2014-2017, 江苏乐博国际投资发展有限公司
 * FileName: FlowBeanSortReducer.java
 * Author:   zhangdanji
 * Date:     2017年05月31日
 * Description:   
 */
package com.mychebao.hadoop.reducer;

import com.mychebao.hadoop.pojo.FlowBean;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author zhangdanji
 */
public class FlowBeanSortReducer extends Reducer<FlowBean,NullWritable,Text,FlowBean>{

    @Override
    protected void reduce(FlowBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        context.write(new Text(key.getPhoneNbr()),key);
    }
}
