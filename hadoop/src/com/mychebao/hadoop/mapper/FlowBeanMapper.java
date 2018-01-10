/*
 * Copyright (C), 2014-2017, 江苏乐博国际投资发展有限公司
 * FileName: FlowBeanMapper.java
 * Author:   zhangdanji
 * Date:     2017年05月31日
 * Description:   
 */
package com.mychebao.hadoop.mapper;

import com.mychebao.hadoop.pojo.FlowBean;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author zhangdanji
 */
public class FlowBeanMapper extends Mapper<LongWritable,Text,Text,FlowBean>{

    private FlowBean flowBean = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            // 拿到一行数据
            String line = value.toString();
            // 切分字段
            String[] fields = StringUtils.split(line, "\t");
            // 拿到我们需要的若干个字段
            String phoneNbr = fields[1];
            long up_flow = Long.parseLong(fields[fields.length - 3]);
            long d_flow = Long.parseLong(fields[fields.length - 2]);
            // 将数据封装到一个flowbean中
            flowBean.set(phoneNbr, up_flow, d_flow);
            new Text("ssssss").find("aaaa");
            // 以手机号为key，将流量数据输出去
            context.write(new Text(phoneNbr), flowBean);
        }catch(Exception e){
            System.out.println("exception occured in mapper" );
        }
    }
}
