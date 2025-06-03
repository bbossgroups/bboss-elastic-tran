package org.frameworkset.tran.output.minio;
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

import okhttp3.OkHttpClient;
import org.frameworkset.tran.output.BaseRemoteConfig;

/**
 * <p>Description: 文件写入minio oss数据库配置</p>
 * <p></p>
 *
 * @author biaoping.yin
 * @Date 2024/8/9
 */
public class OSSFileConfig extends BaseRemoteConfig {

    private String name;
    private String endpoint;
    private String accessKeyId;
    private String secretAccesskey;
    private String region;
    private OkHttpClient httpClient;
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


    private OSSInfoBuilder ossInfoBuilder;
    
    public void init(){
        if(ossInfoBuilder == null){
            ossInfoBuilder = new DefaultOSSInfoBuilder();
        }
    }
    
    public String getName() {
        return name;
    }

    public OSSFileConfig setName(String name) {
        this.name = name;
        return this;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public OSSFileConfig setEndpoint(String endpoint) {
        this.endpoint = endpoint;
        return this;
    }

    public String getAccessKeyId() {
        return accessKeyId;
    }

    public OSSFileConfig setAccessKeyId(String accessKeyId) {
        this.accessKeyId = accessKeyId;
        return this;
    }

    public String getSecretAccesskey() {
        return secretAccesskey;
    }

    public OSSFileConfig setSecretAccesskey(String secretAccesskey) {
        this.secretAccesskey = secretAccesskey;
        return this;
    }

    public String getRegion() {
        return region;
    }

    public OSSFileConfig setRegion(String region) {
        this.region = region;
        return this;
    }

    public OkHttpClient getHttpClient() {
        return httpClient;
    }

    public OSSFileConfig setHttpClient(OkHttpClient httpClient) {
        this.httpClient = httpClient;
        return this;
    }

    public long getMaxFilePartSize() {
        return maxFilePartSize;
    }

    public OSSFileConfig setMaxFilePartSize(long maxFilePartSize) {
        this.maxFilePartSize = maxFilePartSize;
        return this;
    }

    public long getConnectTimeout() {
        return connectTimeout;
    }

    public OSSFileConfig setConnectTimeout(long connectTimeout) {
        this.connectTimeout = connectTimeout;
        return this;
    }

    public long getReadTimeout() {
        return readTimeout;
    }

    public OSSFileConfig setReadTimeout(long readTimeout) {
        this.readTimeout = readTimeout;
        return this;
    }

    public long getWriteTimeout() {
        return writeTimeout;
    }

    public OSSFileConfig setWriteTimeout(long writeTimeout) {
        this.writeTimeout = writeTimeout;
        return this;
    }
    public OSSInfoBuilder getOssInfoBuilder() {
        return ossInfoBuilder;
    }

    public OSSFileConfig setOssInfoBuilder(OSSInfoBuilder ossInfoBuilder) {
        this.ossInfoBuilder = ossInfoBuilder;
        return this;        
    }

    public String getBucket() {
        return bucket;
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }

    public long getSocketTimeout() {
        return socketTimeout;
    }

    public void setSocketTimeout(long socketTimeout) {
        this.socketTimeout = socketTimeout;
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
}
