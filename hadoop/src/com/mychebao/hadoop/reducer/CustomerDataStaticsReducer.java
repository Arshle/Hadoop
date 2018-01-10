/*
 * Copyright (C), 2014-2017, 江苏乐博国际投资发展有限公司
 * FileName: CustomerDataStaticsReducer.java
 * Author:   zhangdanji
 * Date:     2017年05月31日
 * Description: 用户上网数据  
 */
package com.mychebao.hadoop.reducer;

import com.mychebao.hadoop.pojo.CustomerWritable;
import com.mychebao.hadoop.util.Log;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

/**
 * 用户上网数据
 *
 * @author zhangdanji
 */
public class CustomerDataStaticsReducer extends Reducer<Text,CustomerWritable,Text,CustomerWritable>{

    private CustomerWritable tCustomer = new CustomerWritable();

    @Override
    protected void reduce(Text key, Iterable<CustomerWritable> values, Context context) throws IOException, InterruptedException {

        long totalUploadDataPackage = 0;
        long totalDownloadDataPackage = 0;
        long totalUploadFlow = 0;
        long totalDownloadFlow = 0;
        for(CustomerWritable customer : values){
            totalUploadDataPackage += customer.getUploadDataPackage();
            totalDownloadDataPackage += customer.getDownloadDataPackage();
            totalUploadFlow += customer.getUploadFlow();
            totalDownloadFlow += customer.getDownloadFlow();
        }
        Log.info("key:" + key + ",totalUploadDataPackage:" + totalUploadDataPackage + ",totalDownloadDataPackage:" + totalDownloadDataPackage + ",totalUploadFlow:" + totalUploadFlow + ",totalDownloadFlow:" + totalDownloadFlow);
        tCustomer.setUploadDataPackage(totalUploadDataPackage);
        tCustomer.setDownloadDataPackage(totalDownloadDataPackage);
        tCustomer.setUploadFlow(totalUploadFlow);
        tCustomer.setDownloadFlow(totalDownloadFlow);
        context.write(key,tCustomer);
    }
}
