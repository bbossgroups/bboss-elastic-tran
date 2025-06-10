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
import org.frameworkset.nosql.s3.OSSClient;
import org.frameworkset.nosql.s3.OSSConfig;
import org.frameworkset.nosql.s3.OSSHelper;
import org.frameworkset.nosql.s3.OSSStartResult;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.input.file.FilelogPluginException;
import org.frameworkset.tran.output.s3.OSSFileConfig;
import org.frameworkset.tran.output.s3.OSSFileInfo;
import org.frameworkset.tran.output.s3.OSSInfoBuilder;
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
public class OSSSendFileFunction implements SendFileFunction{
    private Logger logger = LoggerFactory.getLogger(OSSSendFileFunction.class);
    private FileOutputConfig fileOutputConfig;
    private OSSStartResult ossStartResult;
    private OSSClient ossClient ;
    private OSSFileConfig ossFileConfig;
    private OSSInfoBuilder ossInfoBuilder;
    private ImportContext importContext;
    public OSSSendFileFunction(ImportContext importContext, FileOutputConfig fileOutputConfig){
        this.fileOutputConfig = fileOutputConfig;
        ossFileConfig = fileOutputConfig.getOssFileConfig();
        this.importContext = importContext;
    }
    public void init(){
        ossFileConfig.init();
        ossInfoBuilder = ossFileConfig.getOssInfoBuilder();
        if(SimpleStringUtil.isNotEmpty(ossFileConfig.getEndpoint())) {
            OSSConfig ossConfig = new OSSConfig();
            ossConfig.setName(ossFileConfig.getName());
            ossConfig.setAccessKeyId(ossFileConfig.getAccessKeyId());
            ossConfig.setEndpoint(ossFileConfig.getEndpoint());

            ossConfig.setConnectTimeout(ossFileConfig.getConnectTimeout());
//            ossConfig.setHttpClient(OSSFileConfig.getHttpClient());
            ossConfig.setMaxFilePartSize(ossFileConfig.getMaxFilePartSize());
            ossConfig.setReadTimeout(ossFileConfig.getReadTimeout());
            ossConfig.setSecretAccesskey(ossFileConfig.getSecretAccesskey());
            ossConfig.setWriteTimeout(ossFileConfig.getWriteTimeout());
            ossConfig.setSocketTimeout(ossFileConfig.getSocketTimeout());
            ossConfig.setConnectionAcquisitionTimeout(ossFileConfig.getConnectionAcquisitionTimeout());
            ossConfig.setConnectionMaxIdleTime(ossFileConfig.getConnectionMaxIdleTime());
            ossConfig.setPathStyleAccess(ossFileConfig.getPathStyleAccess());
            ossConfig.setPoolMaxIdleConnections(ossFileConfig.getPoolMaxIdleConnections());
            ossConfig.setTcpKeepAlive(ossFileConfig.getTcpKeepAlive());
            ossConfig.setRegion(ossFileConfig.getRegion());
            boolean result = OSSHelper.init(ossConfig);
            ossClient = OSSHelper.getOSSClientDS(ossFileConfig.getName());
            if(result){
                ossStartResult = new OSSStartResult();
                ossStartResult.addDBStartResult(ossFileConfig.getName());
            }
        }
        
    }
    private Object lock = new Object();
    private void initOSSClient(){
        if(ossClient != null){
            return;
        }
        synchronized (lock){
            if(ossClient == null){
                ossClient = OSSHelper.getOSSClientDS(ossFileConfig.getName());
            }
        }
    }
    
    @Override
    public void sendFile(FileOutputConfig fileOutputConfig, String filePath, String remoteFilePath,boolean resend) {
        File file = new File(filePath);
        OSSFileInfo ossFileInfo = ossInfoBuilder.buildOSSFileInfo(ossFileConfig,file);
        try {
            initOSSClient();            
            long s = System.currentTimeMillis();
            ossClient.createBucket(ossFileInfo.getBucket());
            ossClient.uploadObject(filePath,ossFileInfo.getBucket(),ossFileInfo.getId());
            long e = System.currentTimeMillis();
            String msg = null;
            if(!resend) {
                msg = "Send file "+filePath+" to oss bucket "+ossFileInfo.getBucket()+" complete,耗时:"+(e-s)+"毫秒";
            }
            else{
                msg = "Resend file "+filePath+" to oss bucket "+ossFileInfo.getBucket()+" complete,耗时:"+(e-s)+"毫秒";
               
            }
            logger.info(msg);
            importContext.reportJobMetricLog(msg);
        } catch (Exception e) {
            throw new FilelogPluginException("Send File failed:"+ossFileInfo.toString(),e);
        }
        
    }

    public void close(){
        if(ossStartResult != null){
            OSSHelper.shutdown(ossStartResult);
        }
    }
}
