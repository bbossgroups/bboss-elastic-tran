package org.frameworkset.tran.jobflow;
/**
 * Copyright 2025 bboss
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

import org.frameworkset.tran.ftp.FtpConfig;
import org.frameworkset.tran.jobflow.context.JobFlowNodeExecuteContext;
import org.frameworkset.tran.jobflow.scan.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author biaoping.yin
 * @Date 2025/9/18
 */
public class DownloadJobFlowNodeFunction implements JobFlowNodeFunction{

    private static Logger logger = LoggerFactory.getLogger(DownloadJobFlowNodeFunction.class);
    private JobFlowNode jobFlowNode;
    private FileDownloadService fileDownloadService;
//    private FtpConfig ftpConfig;
//    private OSSFileInputConfig ossFileInputConfig;

    private BuildDownloadConfigFunction buildDownloadConfigFunction;
    private DownloadfileConfig downloadfileConfig;

    private DownloadedFileRecorder downloadedFileRecorder;
    

 

    public JobFileFilter getFileFilter() {
        return downloadfileConfig.getFileFilter();
    }

    public boolean isScanChild() {
        return downloadfileConfig.isScanChild();
    }

    public DownloadedFileRecorder getDownloadedFileRecord() {
        return downloadedFileRecorder;
    }

    public DownloadfileConfig getDownloadfileConfig() {
        return downloadfileConfig;
    }

    public void setBuildDownloadConfigFunction(BuildDownloadConfigFunction buildDownloadConfigFunction) {
        this.buildDownloadConfigFunction = buildDownloadConfigFunction;
    }

    public void setDownloadedFileRecord(DownloadedFileRecorder downloadedFileRecorder) {
        this.downloadedFileRecorder = downloadedFileRecorder;
    }
    
    public void setDownloadfileConfig(DownloadfileConfig downloadfileConfig) {
        this.downloadfileConfig = downloadfileConfig;
    }

    /**
     * 初始化构建节点函数实例时，只在构建工作流节点时调用一次
     *
     * @param jobFlowNode
     */
    @Override
    public void init(JobFlowNode jobFlowNode) {
        this.jobFlowNode = jobFlowNode;
//        ftpConfig = remoteFileInputJobFlowNodeBuilder.getFtpConfig();
//        ossFileInputConfig = remoteFileInputJobFlowNodeBuilder.getOSSFileInputConfig();
        fileDownloadService = new FileDownloadService(this);
        if(buildDownloadConfigFunction == null && this.downloadfileConfig != null){
            downloadfileConfig.init();
        }
        if(downloadedFileRecorder == null){
            downloadedFileRecorder = new DefaultDownloadedFileRecorder();
        }
        
    }
    private void initDownloadfileConfig(JobFlowNodeExecuteContext jobFlowNodeExecuteContext){
        DownloadfileConfig _downloadfileConfig = null;
        if(buildDownloadConfigFunction != null){
            _downloadfileConfig = buildDownloadConfigFunction.buildDownloadConfig(jobFlowNodeExecuteContext);
            _downloadfileConfig.init();
            downloadfileConfig = _downloadfileConfig;
        }
        
    }
    private JobLogDirScan logDirScanThread(JobFlowNodeExecuteContext jobFlowNodeExecuteContext){
       
        JobLogDirScan jobLogDirScan = null;
       
        
        if (downloadfileConfig.getFtpConfig() != null) {
//            FtpConfig ftpConfig = (FtpConfig) fileConfig;
            if(downloadfileConfig.getFtpConfig().getTransferProtocol() == FtpConfig.TRANSFER_PROTOCOL_FTP) {
                jobLogDirScan = new JobFtpLogDirScan(
                        downloadfileConfig,fileDownloadService);
                jobLogDirScan.setRemote(true);
            }
            else{
                jobLogDirScan = new JobSFtpLogDirScan(
                        downloadfileConfig,fileDownloadService);
                jobLogDirScan.setRemote(true);
            }

        } else if (downloadfileConfig.getOssFileInputConfig()  != null) {
            jobLogDirScan = new JobS3DirScan(downloadfileConfig,fileDownloadService);
        } 
        return jobLogDirScan;
    }
    /**
     * 执行工作流函数
     *
     * @param jobFlowNodeExecuteContext
     * @return
     */
    @Override
    public Object call(JobFlowNodeExecuteContext jobFlowNodeExecuteContext) throws Exception {
        initDownloadfileConfig(  jobFlowNodeExecuteContext);
        JobLogDirScan jobLogDirScan = this.logDirScanThread( jobFlowNodeExecuteContext);
        
        
        jobLogDirScan.scanNewFile(  jobFlowNodeExecuteContext);
        return "ok";
    }

    /**
     * 重置一些监控状态
     */
    @Override
    public void reset() {
        logger.info("execute relase");
    }

    /**
     * 释放资源
     */
    @Override
    public void release() {
        if(buildDownloadConfigFunction != null && downloadfileConfig != null){
            downloadfileConfig.destroy();
        }
        logger.info("execute relase");
    }

    /**
     * 停止
     */
    @Override
    public void stop() {
        if(downloadfileConfig != null)
            downloadfileConfig.destroy();
    }
}
