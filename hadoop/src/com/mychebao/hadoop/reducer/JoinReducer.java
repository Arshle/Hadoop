/*
 * Copyright (C), 2014-2017, 江苏乐博国际投资发展有限公司
 * FileName: JoinReducer.java
 * Author:   zhangdanji
 * Date:     2017年08月09日
 * Description: 连接reducer  
 */
package com.mychebao.hadoop.reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * 连接reducer
 *
 * @author zhangdanji
 */
public class JoinReducer extends Reducer<Text,Text,Text,Text> {

    private final static String LEFT_FLAG = "l";
    private final static String RIGHT_FLAG = "r";

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> iterator = values.iterator();
        List<String> studentClassNames = new ArrayList<>();
        String studentName = null;
        while(iterator.hasNext()){
            String[] infos = iterator.next().toString().split("\t");
            if(infos[1].equals(LEFT_FLAG)){
                studentName = infos[1];
            }else if(infos[1].equals(RIGHT_FLAG)){
                studentClassNames.add(infos[1]);
            }
        }
        for(int i = 0; i < studentClassNames.size(); i++){
            context.write(new Text(studentName),new Text(studentClassNames.get(i)));
        }
    }
}
