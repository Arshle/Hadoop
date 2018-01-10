/*
 * Copyright (C), 2014-2017, 江苏乐博国际投资发展有限公司
 * FileName: Log.java
 * Author:   zhangdanji
 * Date:     2017年05月31日
 * Description: 日志  
 */
package com.mychebao.hadoop.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 日志
 *
 * @author zhangdanji
 */
public class Log {

    private static Logger logger = LoggerFactory.getLogger(Log.class);

    public static void debug(String s){
        logger.debug(s);
    }

    public static void info(String s){
        logger.info(s);
    }
}
