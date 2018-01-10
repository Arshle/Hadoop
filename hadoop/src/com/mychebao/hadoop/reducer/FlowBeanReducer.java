/*
 * Copyright (C), 2014-2017, 江苏乐博国际投资发展有限公司
 * FileName: FlowBeanReducer.java
 * Author:   zhangdanji
 * Date:     2017年05月31日
 * Description:   
 */
package com.mychebao.hadoop.reducer;

import com.mychebao.hadoop.pojo.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author zhangdanji
 */
public class FlowBeanReducer extends Reducer<Text,FlowBean,Text,FlowBean>{
    private FlowBean flowBean = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        long up_flow_sum = 0;
        long d_flow_sum = 0;

        for(FlowBean bean:values){

            up_flow_sum += bean.getUp_flow();
            d_flow_sum += bean.getD_flow();

        }

        flowBean.set(key.toString(), up_flow_sum, d_flow_sum);

        context.write(key, flowBean);
    }
}
