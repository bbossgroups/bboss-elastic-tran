package org.frameworkset.tran.plugin.file.output;
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
import org.frameworkset.nosql.minio.Minio;
import org.frameworkset.nosql.minio.MinioConfig;
import org.frameworkset.nosql.minio.MinioHelper;
import org.frameworkset.nosql.minio.MinioStartResult;
import org.frameworkset.tran.input.file.FilelogPluginException;
import org.frameworkset.tran.output.minio.MinioFileConfig;
import org.frameworkset.tran.output.minio.OSSFileInfo;
import org.frameworkset.tran.output.minio.OSSInfoBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * <p>Description: </p>
 * <p></p>
 *
 * @author biaoping.yin
 * @Date 2024/8/9
 */
public class MinioSendFileFunction implements SendFileFunction{
    private Logger logger = LoggerFactory.getLogger(FtpSendFileFunction.class);
    private FileOutputConfig fileOutputConfig;
    private MinioStartResult minioStartResult;
    private Minio minio ;
    private MinioFileConfig minioFileConfig;
    private OSSInfoBuilder ossInfoBuilder;
    public MinioSendFileFunction(FileOutputConfig fileOutputConfig){
        this.fileOutputConfig = fileOutputConfig;
        minioFileConfig = fileOutputConfig.getMinioFileConfig();
    }
    public void init(){
        minioFileConfig.init();
        ossInfoBuilder = minioFileConfig.getOssInfoBuilder();
        if(SimpleStringUtil.isNotEmpty(minioFileConfig.getEndpoint())) {
            MinioConfig minioConfig = new MinioConfig();
            minioConfig.setName(minioFileConfig.getName());
            minioConfig.setAccessKeyId(minioFileConfig.getAccessKeyId());
            minioConfig.setEndpoint(minioFileConfig.getEndpoint());

            minioConfig.setConnectTimeout(minioFileConfig.getConnectTimeout());
            minioConfig.setHttpClient(minioFileConfig.getHttpClient());
            minioConfig.setMaxFilePartSize(minioFileConfig.getMaxFilePartSize());
            minioConfig.setReadTimeout(minioFileConfig.getReadTimeout());
            minioConfig.setSecretAccesskey(minioFileConfig.getSecretAccesskey());
            minioConfig.setWriteTimeout(minioFileConfig.getWriteTimeout());
            boolean result = MinioHelper.init(minioConfig);
            minio = MinioHelper.getMinio(minioFileConfig.getName());
            if(result){
                minioStartResult = new MinioStartResult();
                minioStartResult.addDBStartResult(minioFileConfig.getName());
            }
        }
    }

    @Override
    public void sendFile(FileOutputConfig fileOutputConfig, String filePath, String remoteFilePath,boolean resend) {
        File file = new File(filePath);
        OSSFileInfo ossFileInfo = ossInfoBuilder.buildOSSFileInfo(minioFileConfig,file);
        try {
            minio.createBucket(ossFileInfo.getBucket());
            minio.saveOssFile(file,ossFileInfo.getBucket(),ossFileInfo.getId());
            if(!resend) {
                logger.info("Resend file {} to minio complete.",filePath );
            }
            else{
                logger.info("Send file {} to minio complete.",filePath);
            }
        } catch (Exception e) {
            throw new FilelogPluginException("createBucket failed:"+ossFileInfo.toString(),e);
        }
        
    }

    public void close(){
        if(minioStartResult != null){
            MinioHelper.shutdown(minioStartResult);
        }
    }
}
