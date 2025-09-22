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

import com.frameworkset.util.SimpleStringUtil;
import org.apache.commons.lang3.StringUtils;
import org.frameworkset.tran.ftp.FtpConfig;
import org.frameworkset.tran.input.file.FileConfig;
import org.frameworkset.tran.input.file.FileFilter;
import org.frameworkset.tran.input.file.FilterFileInfo;
import org.frameworkset.tran.input.s3.OSSFileInputConfig;
import org.frameworkset.tran.jobflow.builder.SimpleJobFlowNodeBuilder;
import org.frameworkset.tran.jobflow.context.JobFlowNodeExecuteContext;
import org.frameworkset.tran.jobflow.scan.JobFileFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 文件采集节点构建器，继承SimpleJobFlowNodeBuilder
 * 
 * @author biaoping.yin
 * @Date 2025/9/17
 */
public class RemoteFileInputJobFlowNodeBuilder extends SimpleJobFlowNodeBuilder<RemoteFileInputJobFlowNodeBuilder> {

    private static Logger logger = LoggerFactory.getLogger(RemoteFileInputJobFlowNodeBuilder.class);
    private FtpConfig ftpConfig;
    private OSSFileInputConfig ossFileInputConfig;
    private JobFileFilter jobFileFilter;
    private boolean scanChild;
    private String fileNameRegular;
    private Pattern fileNameRexPattern = null;
    
 
   
 
    /**
     * 文件采集作业节点构建函数
     *
     * @param nodeId
     * @param nodeName
     */
    public RemoteFileInputJobFlowNodeBuilder(String nodeId, String nodeName) {
        super(nodeId, nodeName);
    }

    
    public RemoteFileInputJobFlowNodeBuilder(){
        super();
    }
    /**
     * 构建文件采集函数
     * @return
     */
    @Override
    protected JobFlowNodeFunction buildJobFlowNodeFunction() {
        if(ftpConfig != null){
            if(SimpleStringUtil.isEmpty(ftpConfig.getSourcePath()) ){
                throw new JobFlowException("没有指定下载本地目录");
            }
        }
        if(ossFileInputConfig != null){
            if(SimpleStringUtil.isEmpty(ossFileInputConfig.getSourcePath()) ){
                throw new JobFlowException("没有指定下载本地目录");
            }
        }
        
        if(StringUtils.isNotEmpty(this.fileNameRegular)){
            fileNameRexPattern = Pattern.compile(this.fileNameRegular);
        }
        if(fileNameRexPattern != null && this.jobFileFilter != null){
            logger.warn("Cannot set both fileNameRexPattern and fileFilter at the same time,fileFilter will be used and ignore fileNameRexPattern {}.",this.fileNameRegular);
        }
        if(jobFileFilter == null && fileNameRexPattern != null){
            jobFileFilter = new JobFileFilter() {
                @Override
                public boolean accept(FilterFileInfo fileInfo, JobFlowNodeExecuteContext jobFlowNodeExecuteContext) {
                    Matcher m = fileNameRexPattern.matcher(fileInfo.getFileName());
                    return m.matches();
                }
            };
        }
        if(ftpConfig != null){
            ftpConfig.initJob(this);

        }
        else if(ossFileInputConfig != null){
            ossFileInputConfig.initJob(this);
        }
        DownloadJobFlowNodeFunction downloadJobFlowNodeFunction = new DownloadJobFlowNodeFunction();
        downloadJobFlowNodeFunction.setRemoteFileInputJobFlowNodeBuilder(this);
        
        return downloadJobFlowNodeFunction;
    }

    /**
     * 设置FTP/SFTP远程文件采集配置：包括FTP/SFTP服务器配置，远程文件目录，下载文件目录，下载文件数量，是否删除已经下载的远程文件
     * @param ftpConfig
     * @return
     */
    public RemoteFileInputJobFlowNodeBuilder setFtpConfig(FtpConfig ftpConfig){
        this.ftpConfig = ftpConfig;
        return   this;
    }
    public RemoteFileInputJobFlowNodeBuilder setOSSFileInputConfig(OSSFileInputConfig ossFileInputConfig){
        this.ossFileInputConfig = ossFileInputConfig;
        return     this;
    }
    public RemoteFileInputJobFlowNodeBuilder setFileFilter(JobFileFilter jobFileFilter){
        this.jobFileFilter = jobFileFilter;
        return   this;
    }

    public FtpConfig getFtpConfig() {
        return ftpConfig;
    }
    
    public OSSFileInputConfig getOSSFileInputConfig() {
        return ossFileInputConfig;
    }
    
    public JobFileFilter getFileFilter() {
        return jobFileFilter;
    }
    
    public boolean isScanChild() {
        return scanChild;
    }
    public RemoteFileInputJobFlowNodeBuilder setScanChild(boolean scanChild) {
        this.scanChild = scanChild;
        return this;
    }

    public String getFileNameRegular() {
        return fileNameRegular;
    }

    public RemoteFileInputJobFlowNodeBuilder setFileNameRegular(String fileNameRegular) {
        this.fileNameRegular = fileNameRegular;
        return  this;
    }
    
 
}
