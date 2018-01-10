/*
 * Copyright (C), 2014-2017, 江苏乐博国际投资发展有限公司
 * FileName: AreaPartition.java
 * Author:   zhangdanji
 * Date:     2017年05月31日
 * Description:   
 */
package com.mychebao.hadoop.partition;

import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;
import java.util.Map;

/**
 * @author zhangdanji
 */
public class AreaPartition<KEY,VALUE> extends Partitioner<KEY,VALUE>{

    private static Map<String,Integer> areaMap =  new HashMap<>();

    static{
        areaMap.put("137",0);
        areaMap.put("139",1);
        areaMap.put("183",2);
        areaMap.put("150",3);
    }

    @Override
    public int getPartition(KEY key, VALUE value, int numPartitions) {
        return areaMap.get(key.toString().substring(0,3)) == null ? 4 : areaMap.get(key.toString().substring(0,3));
    }
}
