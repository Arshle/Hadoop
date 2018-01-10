/*
 * Copyright (C), 2014-2017, 江苏乐博国际投资发展有限公司
 * FileName: FlowBeanSortMapper.java
 * Author:   zhangdanji
 * Date:     2017年05月31日
 * Description:   
 */
package com.mychebao.hadoop.mapper;

import com.mychebao.hadoop.pojo.FlowBean;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author zhangdanji
 */
public class FlowBeanSortMapper extends Mapper<LongWritable,Text,FlowBean,NullWritable>{

    private FlowBean flowBean = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            // 拿到一行数据
            String line = value.toString();
            // 切分字段
            String[] fields = StringUtils.split(line, "\t");
            // 拿到我们需要的若干个字段
            String phoneNbr = fields[0];
            long up_flow = Long.parseLong(fields[1]);
            long d_flow = Long.parseLong(fields[2]);
            // 将数据封装到一个flowbean中
            flowBean.set(phoneNbr, up_flow, d_flow);

            // 以手机号为key，将流量数据输出去
            context.write(flowBean, NullWritable.get());
        }catch(Exception e){
            System.out.println("exception occured in mapper" );
        }
    }
}
