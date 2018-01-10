/*
 * Copyright (C), 2014-2017, 江苏乐博国际投资发展有限公司
 * FileName: CustomerDataStatisticsMapper.java
 * Author:   zhangdanji
 * Date:     2017年05月31日
 * Description: 用户上网信息统计  
 */
package com.mychebao.hadoop.mapper;

import com.mychebao.hadoop.pojo.CustomerWritable;
import com.mychebao.hadoop.util.Log;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

/**
 * 用户上网信息统计
 *
 * @author zhangdanji
 */
public class CustomerDataStatisticsMapper extends Mapper<LongWritable,Text,Text,CustomerWritable>{

    private CustomerWritable customer = new CustomerWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            String[] customerInfos = value.toString().split("\t");
            String mobile = customerInfos[1].trim();
            long uploadDataPackage = Long.parseLong(StringUtils.isEmpty(customerInfos[6].trim())?"0":customerInfos[6].trim());
            long downloadDataPackage = Long.parseLong(StringUtils.isEmpty(customerInfos[7].trim())?"0":customerInfos[7].trim());
            long uploadFlow = Long.parseLong(StringUtils.isEmpty(customerInfos[8].trim())?"0":customerInfos[8].trim());
            long downloadFlow = Long.parseLong(StringUtils.isEmpty(customerInfos[9].trim())?"0":customerInfos[9].trim());
            customer.setMobile(mobile);
            customer.setUploadDataPackage(uploadDataPackage);
            customer.setDownloadDataPackage(downloadDataPackage);
            customer.setUploadFlow(uploadFlow);
            customer.setDownloadFlow(downloadFlow);
            Log.info("key:" + mobile + ",uploadDataPackage:" + uploadDataPackage + ",downloadDataPackage:" + downloadDataPackage + ",uploadFlow:" + uploadFlow + ",downloadFlow:" + downloadFlow);
            context.write(new Text(mobile),customer);
        }catch (Exception e){
            e.printStackTrace();
            Log.info(key + "行数据解析出错");
        }
    }
}
