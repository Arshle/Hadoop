/*
 * Copyright (C), 2014-2017, 江苏乐博国际投资发展有限公司
 * FileName: WordCountJob.java
 * Author:   zhangdanji
 * Date:     2017年05月28日
 * Description: 测试Job  
 */
package com.mychebao.hadoop.util;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.lang.reflect.ParameterizedType;

/**
 * 测试Job
 *
 * @author zhangdanji
 */
public class MRJobUtil {

    private static Job job; //Hadoop Job

    /**
     * 创建Hdfs分区Job
     *
     * @param mapperClass mapper类
     * @param reducerClass reducer类
     * @param inputPath 元数据路径
     * @param outputPath 结果输出路径
     * @param partitionClass 分区类
     * @param numReducerTask Reducer数量
     * @return boolean
     *
     * **/
    public static boolean createHdfsPartitionJob(Class<? extends Partitioner> partitionClass,int numReducerTask,Class<? extends Mapper> mapperClass, Class<? extends Reducer> reducerClass, String inputPath, String outputPath){
        Log.info("---Begin Invoke:MRJobUtil.createHdfsPartitionJob---");
        try {
            setHdfsJob(mapperClass,reducerClass,inputPath,outputPath);
            setPartitionJob(partitionClass,numReducerTask);
            Log.info("---Start Wait For Partition HDFS Job Completion---");
            return job.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
            Log.info("---createHdfsPartitionJob Failure---");
            return false;
        }
    }

    /**
     * 创建Local分区Job
     *
     * @param mapperClass mapper类
     * @param reducerClass reducer类
     * @param inputPath 元数据路径
     * @param outputPath 结果输出路径
     * @param partitionClass 分区类
     * @param numReducerTask Reducer数量
     * @return boolean
     *
     * **/
    public static boolean createLocalPartitionJob(Class<? extends Partitioner> partitionClass,int numReducerTask,Class<? extends Mapper> mapperClass, Class<? extends Reducer> reducerClass, String inputPath, String outputPath){
        Log.info("---Begin Invoke:MRJobUtil.createLocalPartitionJob---");
        try {
            setLocalJob(mapperClass,reducerClass,inputPath,outputPath);
            setPartitionJob(partitionClass,numReducerTask);
            Log.info("---Start Wait For Partition Local Job Completion---");
            return job.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
            Log.info("---createLocalPartitionJob Failure---");
            return false;
        }
    }

    /**
     * 创建Hdfs Job
     *
     * @param mapperClass mapper类
     * @param reducerClass reducer类
     * @param inputPath 元数据路径
     * @param outputPath 结果输出路径
     * @return boolean
     *
     * **/
    public static boolean createHdfsJob(Class<? extends Mapper> mapperClass, Class<? extends Reducer> reducerClass, String inputPath, String outputPath){
        Log.info("---Begin Invoke:MRJobUtil.createReduceJob,param{mapperClass:" + mapperClass.getName() + ",reducerClass:" + reducerClass.getName() + ",inputPath:" + inputPath + ",outputPath:" + outputPath + "}---");
        try {
            setHdfsJob(mapperClass,reducerClass,inputPath,outputPath);
            Log.info("---Start Wait For HDFS Job Completion---");
            return job.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
            Log.info("---createReduceJob Failure---");
            return false;
        }
    }

    /**
     * 创建Local Job
     *
     * @param mapperClass mapper类
     * @param reducerClass reducer类
     * @param inputPath 元数据路径
     * @param outputPath 结果输出路径
     * @return boolean
     *
     * **/
    public static boolean createLocalReduceJob(Class<? extends Mapper> mapperClass, Class<? extends Reducer> reducerClass, String inputPath, String outputPath){
        Log.info("---Begin Invoke:MRJobUtil.createReduceJob,param{mapperClass:" + mapperClass.getName() + ",reducerClass:" + reducerClass.getName() + ",inputPath:" + inputPath + ",outputPath:" + outputPath + "}---");
        try {
            setLocalJob(mapperClass,reducerClass,inputPath,outputPath);
            Log.info("---Start Wait For Local Job Completion---");
            return job.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
            Log.info("---createReduceJob Failure---");
            return false;
        }
    }

    /**
     * 设置job原始参数
     *
     * @param job Hadoop Job
     * @param mapperClass mapper类
     * @param reducerClass reducer类
     *
     * **/
    private static void setOriginalJob(Job job,Class<? extends Mapper> mapperClass,Class<? extends Reducer> reducerClass){
        Log.info("---Begin Invoke:MRJobUtil.setOriginalJob---");
        try {
            //定位到jar包
            job.setJarByClass(MRJobUtil.class);
            //Mapper
            job.setMapperClass(mapperClass);
            //Reducer
            job.setReducerClass(reducerClass);
            //Mapper输出Key的class类型
            job.setMapOutputKeyClass(getOutKeyClass(mapperClass));
            //Mapper输出Value的class类型
            job.setMapOutputValueClass(getOutValueClass(mapperClass));
            //Reducer输出Key的class类型
            job.setOutputKeyClass(getOutKeyClass(reducerClass));
            //Reducer输出Value的class类型
            job.setOutputValueClass(getOutValueClass(reducerClass));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 设置HDFS Job参数
     *
     * @param mapperClass mapper类
     * @param reducerClass reducer类
     * @param inputPath 元数据路径
     * @param outputPath 结果输出路径
     *
     * **/
    private static void setHdfsJob(Class<? extends Mapper> mapperClass,Class<? extends Reducer> reducerClass,String inputPath,String outputPath){
        try {
            //创建HDFS驱动配置Job
            job = Job.getInstance(HadoopHdfsUtil.getConfiguration());
            setOriginalJob(job,mapperClass,reducerClass);
            //HDFS路径
            FileInputFormat.setInputPaths(job,"hdfs://nameservice" + inputPath);
            FileOutputFormat.setOutputPath(job,new Path("hdfs://nameservice" + outputPath));
        } catch (Exception e) {
            e.printStackTrace();
            Log.info("---setHdfsJob Failure---");
        }
    }

    /**
     * 设置Local Job参数
     *
     * @param mapperClass mapper类
     * @param reducerClass reducer类
     * @param inputPath 元数据路径
     * @param outputPath 结果输出路径
     *
     * **/
    private static void setLocalJob(Class<? extends Mapper> mapperClass,Class<? extends Reducer> reducerClass,String inputPath,String outputPath){
        try {
            //创建本地驱动配置Job
            job = Job.getInstance(HadoopHdfsUtil.getConfiguration());
            setOriginalJob(job,mapperClass,reducerClass);
            //本地路径
            FileInputFormat.setInputPaths(job,inputPath);
            FileOutputFormat.setOutputPath(job,new Path(outputPath));
        } catch (Exception e) {
            e.printStackTrace();
            Log.info("---setLocalJob Failure---");
        }
    }

    /**
     * 设置分区参数
     *
     * @param partitionClass 分区类
     * @param numReducerTask reducer数量
     *
     * **/
    private static void setPartitionJob(Class<? extends Partitioner> partitionClass,int numReducerTask){

        try {
            job.setPartitionerClass(partitionClass);
            job.setNumReduceTasks(numReducerTask);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     *
     * 获取输出key的class类型
     *
     * @param clazz 传入Class
     * @return Class<T>
     * @throws Exception 类型转化异常
     *
     * **/
    @SuppressWarnings("unchecked")
    private static <T> Class<T> getOutKeyClass(Class<T> clazz) throws Exception{

        //拿到泛型的第三个类型
        return (Class<T>)((ParameterizedType)clazz
                .getGenericSuperclass()).getActualTypeArguments()[2];
    }

    /**
     *
     * 获取输出value的class类型
     *
     * @param clazz 传入class
     * @return Class<T>
     * @throws Exception 类型转化异常
     *
     * **/
    @SuppressWarnings("unchecked")
    private static <T> Class<T> getOutValueClass(Class<T> clazz) throws Exception{

        //拿到泛型的第四个类型
        return (Class<T>)((ParameterizedType)clazz
                .getGenericSuperclass()).getActualTypeArguments()[3];
    }
}
