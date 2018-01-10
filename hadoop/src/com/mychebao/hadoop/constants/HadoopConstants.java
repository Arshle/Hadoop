/*
 * Copyright (C), 2014-2017, 江苏乐博国际投资发展有限公司
 * FileName: HdfsConstants.java
 * Author:   zhangdanji
 * Date:     2017年05月27日
 * Description: HDFS全局变量  
 */
package com.mychebao.hadoop.constants;

import org.apache.hadoop.hdfs.DistributedFileSystem;

/**
 * HDFS全局变量
 *
 * @author zhangdanji
 */
public class HadoopConstants {

    public final static String DEFAULT_FS_PROP = "fs.defaultFS"; //默认HDFS文件系统参数名

    public final static String DEFAULT_FS_NAMESERVICE = "hdfs://nameservice"; //HDFS nameservice地址

    public final static String FS_NAMESERVICE_PROP = "dfs.nameservices"; //指定nameservice

    public final static String FS_IMPL_PROP = "fs.hdfs.impl"; //指定HDFS执行类参数名

    public final static String FS_IMPL_VALUE = DistributedFileSystem.class.getName(); //指定HDFS执行类

    public final static String FS_REPLICATION_PROP = "dfs.replication";

    public final static String FS_REPLICATION_VALUE = "3";

    public final static String FS_NAMESERVICES_NAME = "nameservice";

    public final static String FS_HA_NAMESERVICE_PROP = "dfs.ha.namenodes.nameservice";

    public final static String FS_NAMENODE1_RPC_ADDRESS = "dfs.namenode.rpc-address.nameservice.namenode1";

    public final static String FS_NAMENODE2_RPC_ADDRESS = "dfs.namenode.rpc-address.nameservice.namenode2";

    public final static String FS_MSTER1_ADRRESS = "172.16.45.184:9000";

    public final static String FS_MSTER2_ADRRESS = "172.16.45.203:9000";

    public final static String FS_NAMESERVICE_NAMENODES = "namenode1,namenode2";

    public final static String FS_FAILOVER_PROVIDER = "dfs.client.failover.proxy.provider.nameservice";

    public final static String FS_HA_NAMENODE_PROXY = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider";

    public final static String HBASE_ZK_LOCATION_PROP = "hbase.zookeeper.quorum";

    public final static String HBASE_ZK_LOCATION_VALUE = "follower1:2181,follower2:2181,follower3:2181";

}
