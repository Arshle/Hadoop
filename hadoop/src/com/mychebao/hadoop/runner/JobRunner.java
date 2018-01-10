/*
 * Copyright (C), 2014-2017, 江苏乐博国际投资发展有限公司
 * FileName: Test.java
 * Author:   zhangdanji
 * Date:     2017年05月29日
 * Description: 测试类  
 */
package com.mychebao.hadoop.runner;

import com.mychebao.hadoop.mapper.JoinMapper;
import com.mychebao.hadoop.mapper.MaxTemperatureMapper;
import com.mychebao.hadoop.reducer.JoinReducer;
import com.mychebao.hadoop.reducer.MaxTemperatureReducer;
import com.mychebao.hadoop.util.MRJobUtil;
import org.apache.hadoop.mapred.jobcontrol.JobControl;

/**
 * 测试类
 *
 * @author zhangdanji
 */
public class JobRunner{

    public static void main(String[] args) {
        MRJobUtil.createHdfsJob(JoinMapper.class, JoinReducer.class,"/data","/data/result");
    }
}
