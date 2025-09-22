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
import org.frameworkset.tran.input.s3.OSSFileInputConfig;
import org.frameworkset.tran.jobflow.context.JobFlowNodeExecuteContext;
import org.frameworkset.tran.jobflow.scan.*;

/**
 * @author biaoping.yin
 * @Date 2025/9/18
 */
public class DownloadJobFlowNodeFunction implements JobFlowNodeFunction{

    private RemoteFileInputJobFlowNodeBuilder remoteFileInputJobFlowNodeBuilder;
    private JobFlowNode jobFlowNode;
    private FileDownloadService fileDownloadService;
    private FtpConfig ftpConfig;
    private OSSFileInputConfig ossFileInputConfig;

    public void setRemoteFileInputJobFlowNodeBuilder(RemoteFileInputJobFlowNodeBuilder remoteFileInputJobFlowNodeBuilder) {
        this.remoteFileInputJobFlowNodeBuilder = remoteFileInputJobFlowNodeBuilder;
    }

    public JobFileFilter getFileFilter() {
        return remoteFileInputJobFlowNodeBuilder.getFileFilter();
    }

    public boolean isScanChild() {
        return remoteFileInputJobFlowNodeBuilder.isScanChild();
    }
     
    public String getFileNameRegular() {
        return remoteFileInputJobFlowNodeBuilder.getFileNameRegular();
    }

    public RemoteFileInputJobFlowNodeBuilder getRemoteFileInputJobFlowNodeBuilder() {
        return remoteFileInputJobFlowNodeBuilder;
    }

    /**
     * 初始化构建节点函数实例时，只在构建工作流节点时调用一次
     *
     * @param jobFlowNode
     */
    @Override
    public void init(JobFlowNode jobFlowNode) {
        this.jobFlowNode = jobFlowNode;
        ftpConfig = remoteFileInputJobFlowNodeBuilder.getFtpConfig();
        ossFileInputConfig = remoteFileInputJobFlowNodeBuilder.getOSSFileInputConfig();
        fileDownloadService = new FileDownloadService(this);
        
    }
    private JobLogDirScan logDirScanThread(){
        JobLogDirScan jobLogDirScan = null;
       
        
        if (ftpConfig != null) {
//            FtpConfig ftpConfig = (FtpConfig) fileConfig;
            if(ftpConfig.getTransferProtocol() == FtpConfig.TRANSFER_PROTOCOL_FTP) {
                jobLogDirScan = new JobFtpLogDirScan(
                        ftpConfig,fileDownloadService);
                jobLogDirScan.setRemote(true);
            }
            else{
                jobLogDirScan = new JobSFtpLogDirScan(
                        ftpConfig,fileDownloadService);
                jobLogDirScan.setRemote(true);
            }

        } else if (ossFileInputConfig != null) {
            jobLogDirScan = new JobS3DirScan(ossFileInputConfig,fileDownloadService);
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
        JobLogDirScan jobLogDirScan = this.logDirScanThread();
        
        
        jobLogDirScan.scanNewFile(  jobFlowNodeExecuteContext);
        return "ok";
    }

    /**
     * 重置一些监控状态
     */
    @Override
    public void reset() {

    }

    /**
     * 释放资源
     */
    @Override
    public void release() {
        if(ftpConfig != null){
            ftpConfig.destroy();
        }
        if(ossFileInputConfig != null){
            ossFileInputConfig.destroy();
        }
    }

    /**
     * 停止
     */
    @Override
    public void stop() {

    }
}
