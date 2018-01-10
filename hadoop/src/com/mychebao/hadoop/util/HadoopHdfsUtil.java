/*
 * Copyright (C), 2014-2017, 江苏乐博国际投资发展有限公司
 * FileName: HadoopHdfsUtil.java
 * Author:   zhangdanji
 * Date:     2017年05月25日
 * Description: HDFS操作工具类  
 */
package com.mychebao.hadoop.util;

import com.mychebao.hadoop.constants.HadoopConstants;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Lz4Codec;

import java.io.*;

/**
 * HDFS操作工具类
 *
 * @author zhangdanji
 */
public class HadoopHdfsUtil {

    private static Configuration configuration; //HDFS配置文件
    private static FileSystem fileSystem; //HDFS文件系统


    public static Configuration getConfiguration(){
        Log.info("---Begin Invoke:HadoopHdfsUtil.getConfiguration---");
        try {
            if(configuration == null){
                synchronized (HadoopHdfsUtil.class){
                    if(configuration == null){
                        configuration = new Configuration();
                        configuration.set(HadoopConstants.DEFAULT_FS_PROP, HadoopConstants.DEFAULT_FS_NAMESERVICE);
                        configuration.set(HadoopConstants.FS_NAMESERVICE_PROP, HadoopConstants.FS_NAMESERVICES_NAME);
                        configuration.set(HadoopConstants.FS_HA_NAMESERVICE_PROP, HadoopConstants.FS_NAMESERVICE_NAMENODES);
                        configuration.set(HadoopConstants.FS_NAMENODE1_RPC_ADDRESS, HadoopConstants.FS_MSTER1_ADRRESS);
                        configuration.set(HadoopConstants.FS_NAMENODE2_RPC_ADDRESS, HadoopConstants.FS_MSTER2_ADRRESS);
                        configuration.set(HadoopConstants.FS_FAILOVER_PROVIDER, HadoopConstants.FS_HA_NAMENODE_PROXY);
                        configuration.set(HadoopConstants.FS_IMPL_PROP, HadoopConstants.FS_IMPL_VALUE);
                        configuration.set(HadoopConstants.FS_REPLICATION_PROP, HadoopConstants.FS_REPLICATION_VALUE);
                    }
                }
            }
            Log.info("---getConfiguration Success---");
        } catch (Exception e) {
            e.printStackTrace();
            Log.info("---getConfiguration Failure---");
        }
        return configuration;
    }

    /**
     *
     * 上传文件至HDFS文件系统
     *
     * @param filePath 文件路径
     * @param uploadPath HDFS路径
     * @return boolean
     *
     * **/
    public static boolean uploadHdfsFile(String filePath,String uploadPath){
        try {
            fileSystem = FileSystem.get(getConfiguration());
            //fileSystem.copyFromLocalFile(new Path(filePath),new Path(uploadPath));
            //fileSystem.delete(new Path(uploadPath),true); //递归删除
            FSDataOutputStream fsDos = fileSystem.create(new Path(HadoopConstants.DEFAULT_FS_NAMESERVICE + uploadPath));
            InputStream is = new FileInputStream(filePath);
            IOUtils.copy(is,fsDos);
            Log.info("---uploadHdfsFile Success---");
            return true;
        }catch (Exception e){
            e.printStackTrace();
            Log.info("---uploadHdfsFile Failure---");
            return false;
        }
    }

    /**
     *
     * 下载HDFS文件
     *
     *
     * **/
    public static boolean downloadHdfsFile(String downloadPath,String hdfsFilePath){
        try {
            fileSystem = FileSystem.get(getConfiguration());
            FSDataInputStream fsDis = fileSystem.open(new Path(HadoopConstants.DEFAULT_FS_NAMESERVICE + hdfsFilePath));
            CompressionCodecFactory factory = new CompressionCodecFactory(getConfiguration());
            CompressionCodec codec = factory.getCodec(new Path(HadoopConstants.DEFAULT_FS_NAMESERVICE + hdfsFilePath));
            InputStream in = codec.createInputStream(fsDis);
            OutputStream os = new FileOutputStream(downloadPath);
            IOUtils.copy(in,os);
            Log.info("---downloadHdfsFile Success---");
            org.apache.hadoop.io.IOUtils.closeStream(fsDis);
            org.apache.hadoop.io.IOUtils.closeStream(os);
            return true;
        }catch (Exception e){
            e.printStackTrace();
            Log.info("---downloadHdfsFile Failure---");
            return false;
        }
    }

    /**
     *
     * 查看文件状态
     *
     *
     * **/
    public static FileStatus getFileStatus(String hdfsFilePath){
        try {
            return FileSystem.get(getConfiguration())
                    .getFileStatus(new Path(HadoopConstants.DEFAULT_FS_NAMESERVICE + hdfsFilePath));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static FileStatus[] listStatus(String filePattern,String filter){
        try {
            PathFilter pathFilter;
            return FileSystem.get(getConfiguration()).globStatus(new Path(HadoopConstants.DEFAULT_FS_NAMESERVICE + filePattern));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static boolean deleteHdfsFile(String hdfsFilePath,boolean recursive){
        try {
            return FileSystem.get(getConfiguration()).delete(new Path(HadoopConstants.DEFAULT_FS_NAMESERVICE + hdfsFilePath),recursive);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    public static void main(String[] args){
        FileStatus fileStatus = getFileStatus("/data/readme.txt");
        System.out.println(fileStatus.getReplication());
    }
}
