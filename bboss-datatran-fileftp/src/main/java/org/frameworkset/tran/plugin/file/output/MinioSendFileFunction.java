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
import org.frameworkset.tran.context.ImportContext;
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
    private Logger logger = LoggerFactory.getLogger(MinioSendFileFunction.class);
    private FileOutputConfig fileOutputConfig;
    private MinioStartResult minioStartResult;
    private Minio minio ;
    private MinioFileConfig minioFileConfig;
    private OSSInfoBuilder ossInfoBuilder;
    private ImportContext importContext;
    public MinioSendFileFunction(ImportContext importContext,FileOutputConfig fileOutputConfig){
        this.fileOutputConfig = fileOutputConfig;
        minioFileConfig = fileOutputConfig.getMinioFileConfig();
        this.importContext = importContext;
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
    private Object lock = new Object();
    private void initMinio(){
        if(minio != null){
            return;
        }
        synchronized (lock){
            if(minio == null){
                minio = MinioHelper.getMinio(minioFileConfig.getName());
            }
        }
    }
    
    @Override
    public void sendFile(FileOutputConfig fileOutputConfig, String filePath, String remoteFilePath,boolean resend) {
        File file = new File(filePath);
        OSSFileInfo ossFileInfo = ossInfoBuilder.buildOSSFileInfo(minioFileConfig,file);
        try {
            initMinio();            
            long s = System.currentTimeMillis();
            minio.createBucket(ossFileInfo.getBucket());
            minio.saveOssFile(file,ossFileInfo.getBucket(),ossFileInfo.getId());
            long e = System.currentTimeMillis();
            String msg = null;
            if(!resend) {
                msg = "Send file "+filePath+" to minio bucket "+ossFileInfo.getBucket()+" complete,耗时:"+(e-s)+"毫秒";
            }
            else{
                msg = "Resend file "+filePath+" to minio bucket "+ossFileInfo.getBucket()+" complete,耗时:"+(e-s)+"毫秒";
               
            }
            logger.info(msg);
            importContext.reportJobMetricLog(msg);
        } catch (Exception e) {
            throw new FilelogPluginException("Send File failed:"+ossFileInfo.toString(),e);
        }
        
    }

    public void close(){
        if(minioStartResult != null){
            MinioHelper.shutdown(minioStartResult);
        }
    }
}
