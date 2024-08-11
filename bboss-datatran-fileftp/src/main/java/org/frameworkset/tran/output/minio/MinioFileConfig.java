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
public class MinioFileConfig extends BaseRemoteConfig {

    private String name;
    private String endpoint;
    private String accessKeyId;
    private String secretAccesskey;
    private String region;
    private OkHttpClient httpClient;
    private long maxFilePartSize = 10485760L;
    private int connectTimeout = 60000;
    private int readTimeout = 60000;
    private int writeTimeout = 60000;
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

    public MinioFileConfig setName(String name) {
        this.name = name;
        return this;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public MinioFileConfig setEndpoint(String endpoint) {
        this.endpoint = endpoint;
        return this;
    }

    public String getAccessKeyId() {
        return accessKeyId;
    }

    public MinioFileConfig setAccessKeyId(String accessKeyId) {
        this.accessKeyId = accessKeyId;
        return this;
    }

    public String getSecretAccesskey() {
        return secretAccesskey;
    }

    public MinioFileConfig setSecretAccesskey(String secretAccesskey) {
        this.secretAccesskey = secretAccesskey;
        return this;
    }

    public String getRegion() {
        return region;
    }

    public MinioFileConfig setRegion(String region) {
        this.region = region;
        return this;
    }

    public OkHttpClient getHttpClient() {
        return httpClient;
    }

    public MinioFileConfig setHttpClient(OkHttpClient httpClient) {
        this.httpClient = httpClient;
        return this;
    }

    public long getMaxFilePartSize() {
        return maxFilePartSize;
    }

    public MinioFileConfig setMaxFilePartSize(long maxFilePartSize) {
        this.maxFilePartSize = maxFilePartSize;
        return this;
    }

    public int getConnectTimeout() {
        return connectTimeout;
    }

    public MinioFileConfig setConnectTimeout(int connectTimeout) {
        this.connectTimeout = connectTimeout;
        return this;
    }

    public int getReadTimeout() {
        return readTimeout;
    }

    public MinioFileConfig setReadTimeout(int readTimeout) {
        this.readTimeout = readTimeout;
        return this;
    }

    public int getWriteTimeout() {
        return writeTimeout;
    }

    public MinioFileConfig setWriteTimeout(int writeTimeout) {
        this.writeTimeout = writeTimeout;
        return this;
    }
    public OSSInfoBuilder getOssInfoBuilder() {
        return ossInfoBuilder;
    }

    public MinioFileConfig setOssInfoBuilder(OSSInfoBuilder ossInfoBuilder) {
        this.ossInfoBuilder = ossInfoBuilder;
        return this;        
    }

    public String getBucket() {
        return bucket;
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }
}
