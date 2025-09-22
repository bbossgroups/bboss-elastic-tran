package org.frameworkset.tran.input.s3;
/**
 * Copyright 2024 bboss
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.frameworkset.util.SimpleStringUtil;
import org.frameworkset.nosql.s3.OSSClient;
import org.frameworkset.nosql.s3.OSSConfig;
import org.frameworkset.nosql.s3.OSSHelper;
import org.frameworkset.nosql.s3.OSSStartResult;
import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.input.RemoteContext;
import org.frameworkset.tran.input.file.FileConfig;
import org.frameworkset.tran.input.file.RemoteFileChannel;
import org.frameworkset.tran.jobflow.RemoteFileInputJobFlowNodeBuilder;
import org.frameworkset.tran.output.s3.DefaultOSSInfoBuilder;
import org.frameworkset.tran.output.s3.OSSInfoBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * <p>Description: oss输入配置</p>
 * <p></p>
 *
 * @author biaoping.yin
 * @Date 2024/8/9
 */
public class OSSFileInputConfig extends RemoteContext<OSSFileInputConfig> {
    private Logger logger = LoggerFactory.getLogger(OSSFileInputConfig.class);

    private String name;
    private String endpoint;
    private String accessKeyId;
    private String secretAccesskey;
    private String region;
    private long maxFilePartSize = 10485760L;
    private long connectTimeout = 60000L;
    private long readTimeout = 60000L;
    private long writeTimeout = 60000L;
    private long socketTimeout = 60000L;
    private Long connectionMaxIdleTime = 600000L;
    private long poolKeepAliveDuration = 300000L;
    private int poolMaxIdleConnections = 5;
    private Boolean pathStyleAccess = true;

    private long connectionAcquisitionTimeout = 10000L;
    private Boolean tcpKeepAlive ;
    private String bucket;
    private int downloadWorkThreads = 3;

    private OSSInfoBuilder ossInfoBuilder;
    private boolean inited;

    private OSSStartResult ossStartResult;
    private OSSClient ossClient ;
    public int getDownloadWorkThreads() {
        return downloadWorkThreads;
    }

    public OSSClient getOssClient() {
        return ossClient;
    }

    public OSSFileInputConfig setFileConfig(FileConfig fileConfig) {
        this.fileConfig = fileConfig;
        return this;
    }

    public FileConfig getFileConfig() {
        return fileConfig;
    }

    public OSSFileInputConfig setDownloadWorkThreads(int downloadWorkThreads) {
        this.downloadWorkThreads = downloadWorkThreads;
        return this;
    }

 
    public void init(FileConfig fileConfig){
        if(inited)
            return;
        inited = true;
        if(ossInfoBuilder == null){
            ossInfoBuilder = new DefaultOSSInfoBuilder();
        }
        downloadTempDir = SimpleStringUtil.getPath(fileConfig.getSourcePath(),"temp");
        fileConfig.setEnableInode(false);
        if(fileConfig.getCloseOlderTime() != null && fileConfig.getCloseOlderTime() > 0L) {
            fileConfig.setCloseEOF(false);//指定CloseOlderTime
        }
        else if(fileConfig.getIgnoreOlderTime() != null && fileConfig.getIgnoreOlderTime() > 0L) {
            fileConfig.setCloseEOF(false);//指定CloseOlderTime
        }
        else{
            logger.info("OSS远程文件采集:setCloseEOF(true)");
            fileConfig.setCloseEOF(true);//已经结束的文件内容采集完毕后关闭文件对应的采集通道，后续不再监听对应文件的内容变化
        }
        File f = new File(downloadTempDir);
        if(!f.exists())
            f.mkdirs();
        initOSSClient();
        if(remoteFileChannel == null && getDownloadWorkThreads() > 0) {
            remoteFileChannel = new RemoteFileChannel();
            //用远程文件路径作为线程池名称
            remoteFileChannel.setThreadName("RemoteFileDownloadChannel-"+getRemoteFileDir());
            remoteFileChannel.setWorkThreads(getDownloadWorkThreads());
            remoteFileChannel.init();

        }
    }

    public void initJob(RemoteFileInputJobFlowNodeBuilder remoteFileInputJobFlowNodeBuilder){
        if(inited)
            return;
        inited = true;
        if(ossInfoBuilder == null){
            ossInfoBuilder = new DefaultOSSInfoBuilder();
        }
        downloadTempDir = SimpleStringUtil.getPath(getSourcePath(),"temp");
        
        File f = new File(downloadTempDir);
        if(!f.exists())
            f.mkdirs();
        initOSSClient();
        super.initJob();
        if(remoteFileChannel == null && getDownloadWorkThreads() > 0) {
            remoteFileChannel = new RemoteFileChannel();
            //用远程文件路径作为线程池名称
            remoteFileChannel.setThreadName("RemoteFileDownloadChannel-"+getRemoteFileDir());
            remoteFileChannel.setWorkThreads(getDownloadWorkThreads());
            remoteFileChannel.init();

        }
    }
    private void initOSSClient(){
        if(SimpleStringUtil.isNotEmpty(getEndpoint())) {
            OSSConfig ossConfig = new OSSConfig();
            ossConfig.setName(getName());
            ossConfig.setAccessKeyId(getAccessKeyId());
            ossConfig.setEndpoint(getEndpoint());

            ossConfig.setConnectTimeout(getConnectTimeout());
//            ossConfig.setHttpClient(OSSFileConfig.getHttpClient());
            ossConfig.setMaxFilePartSize(getMaxFilePartSize());
            ossConfig.setReadTimeout(getReadTimeout());
            ossConfig.setSecretAccesskey(getSecretAccesskey());
            ossConfig.setWriteTimeout(getWriteTimeout());
            ossConfig.setSocketTimeout(getSocketTimeout());
            ossConfig.setConnectionAcquisitionTimeout(getConnectionAcquisitionTimeout());
            ossConfig.setConnectionMaxIdleTime(getConnectionMaxIdleTime());
            ossConfig.setPathStyleAccess(getPathStyleAccess());
            ossConfig.setPoolMaxIdleConnections(getPoolMaxIdleConnections());
            ossConfig.setTcpKeepAlive(getTcpKeepAlive());
            ossConfig.setRegion(getRegion());
            boolean result = OSSHelper.init(ossConfig);
            ossClient = OSSHelper.getOSSClientDS(getName());
            if(result){
                ossStartResult = new OSSStartResult();
                ossStartResult.addDBStartResult(getName());
            }
        }
        else{
            throw new DataImportException("OSS input config check failed: oss endpoint is null!");
        }
    }

    public String getName() {
        return name;
    }
   
    public OSSFileInputConfig setName(String name) {
        this.name = name;
        return this;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public OSSFileInputConfig setEndpoint(String endpoint) {
        this.endpoint = endpoint;
        return this;
    }

    public String getAccessKeyId() {
        return accessKeyId;
    }

    public OSSFileInputConfig setAccessKeyId(String accessKeyId) {
        this.accessKeyId = accessKeyId;
        return this;
    }

    public String getSecretAccesskey() {
        return secretAccesskey;
    }

    public OSSFileInputConfig setSecretAccesskey(String secretAccesskey) {
        this.secretAccesskey = secretAccesskey;
        return this;
    }

    public String getRegion() {
        return region;
    }

    public OSSFileInputConfig setRegion(String region) {
        this.region = region;
        return this;
    }

   

    public long getMaxFilePartSize() {
        return maxFilePartSize;
    }

    public OSSFileInputConfig setMaxFilePartSize(long maxFilePartSize) {
        this.maxFilePartSize = maxFilePartSize;
        return this;
    }

    public long getConnectTimeout() {
        return connectTimeout;
    }

    public OSSFileInputConfig setConnectTimeout(long connectTimeout) {
        this.connectTimeout = connectTimeout;
        return this;
    }

    public long getReadTimeout() {
        return readTimeout;
    }

    public OSSFileInputConfig setReadTimeout(long readTimeout) {
        this.readTimeout = readTimeout;
        return this;
    }

    public long getWriteTimeout() {
        return writeTimeout;
    }

    public OSSFileInputConfig setWriteTimeout(long writeTimeout) {
        this.writeTimeout = writeTimeout;
        return this;
    }
    public OSSInfoBuilder getOssInfoBuilder() {
        return ossInfoBuilder;
    }

    public OSSFileInputConfig setOssInfoBuilder(OSSInfoBuilder ossInfoBuilder) {
        this.ossInfoBuilder = ossInfoBuilder;
        return this;
    }

    public String getBucket() {
        return bucket;
    }

    public OSSFileInputConfig setBucket(String bucket) {
        this.bucket = bucket;
        return this;
    }

    public long getSocketTimeout() {
        return socketTimeout;
    }

    public OSSFileInputConfig setSocketTimeout(long socketTimeout) {
        this.socketTimeout = socketTimeout;
        return this;
    }

    public long getPoolKeepAliveDuration() {
        return poolKeepAliveDuration;
    }

    public void setPoolKeepAliveDuration(long poolKeepAliveDuration) {
        this.poolKeepAliveDuration = poolKeepAliveDuration;
    }

    public long getConnectionAcquisitionTimeout() {
        return connectionAcquisitionTimeout;
    }

    public void setConnectionAcquisitionTimeout(long connectionAcquisitionTimeout) {
        this.connectionAcquisitionTimeout = connectionAcquisitionTimeout;
    }

    public Boolean getTcpKeepAlive() {
        return tcpKeepAlive;
    }

    public void setTcpKeepAlive(Boolean tcpKeepAlive) {
        this.tcpKeepAlive = tcpKeepAlive;
    }

    public Long getConnectionMaxIdleTime() {
        return connectionMaxIdleTime;
    }

    public void setConnectionMaxIdleTime(Long connectionMaxIdleTime) {
        this.connectionMaxIdleTime = connectionMaxIdleTime;
    }

    public Boolean getPathStyleAccess() {
        return pathStyleAccess;
    }

    public void setPathStyleAccess(Boolean pathStyleAccess) {
        this.pathStyleAccess = pathStyleAccess;
    }

    public int getPoolMaxIdleConnections() {
        return poolMaxIdleConnections;
    }

    public void setPoolMaxIdleConnections(int poolMaxIdleConnections) {
        this.poolMaxIdleConnections = poolMaxIdleConnections;
    }
    public void destroy() {
        super.destroy();
        if(ossStartResult != null){
            OSSHelper.shutdown(ossStartResult);
        }
    }

 
}
