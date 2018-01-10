/*
 * Copyright (C), 2014-2017, 江苏乐博国际投资发展有限公司
 * FileName: CustomerWritable.java
 * Author:   zhangdanji
 * Date:     2017年05月31日
 * Description: 客户信息序列化类  
 */
package com.mychebao.hadoop.pojo;

import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 客户信息序列化类
 *
 * @author zhangdanji
 */
public class CustomerWritable implements WritableComparable<CustomerWritable>{

    private String mobile;
    private long uploadDataPackage;
    private long downloadDataPackage;
    private long uploadFlow;
    private long downloadFlow;


    @Override
    public int compareTo(CustomerWritable o) {
        return ((this.uploadFlow + this.downloadFlow) > (o.getUploadFlow() + o.getDownloadFlow()) ? -1 : 1);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.mobile);
        out.writeLong(this.uploadDataPackage);
        out.writeLong(this.downloadDataPackage);
        out.writeLong(this.uploadFlow);
        out.writeLong(this.downloadFlow);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.mobile = in.readUTF();
        this.uploadDataPackage = in.readLong();
        this.downloadDataPackage = in.readLong();
        this.uploadFlow = in.readLong();
        this.downloadFlow = in.readLong();
    }

    @Override
    public String toString() {
        return uploadDataPackage + "\t" + downloadDataPackage + "\t" + uploadFlow + "\t" + downloadFlow;
    }

    public String getMobile() {
        return mobile;
    }

    public void setMobile(String mobile) {
        this.mobile = mobile;
    }

    public long getUploadDataPackage() {
        return uploadDataPackage;
    }

    public void setUploadDataPackage(long uploadDataPackage) {
        this.uploadDataPackage = uploadDataPackage;
    }

    public long getDownloadDataPackage() {
        return downloadDataPackage;
    }

    public void setDownloadDataPackage(long downloadDataPackage) {
        this.downloadDataPackage = downloadDataPackage;
    }

    public long getUploadFlow() {
        return uploadFlow;
    }

    public void setUploadFlow(long uploadFlow) {
        this.uploadFlow = uploadFlow;
    }

    public long getDownloadFlow() {
        return downloadFlow;
    }

    public void setDownloadFlow(long downloadFlow) {
        this.downloadFlow = downloadFlow;
    }
}
