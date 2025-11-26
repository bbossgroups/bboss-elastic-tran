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
import org.frameworkset.tran.input.file.FilterFileInfo;
import org.frameworkset.tran.input.s3.OSSFileInputConfig;
import org.frameworkset.tran.jobflow.context.JobFlowNodeExecuteContext;
import org.frameworkset.tran.jobflow.lifecycle.LifecycleJobFileFilter;
import org.frameworkset.tran.jobflow.scan.JobFileFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author biaoping.yin
 * @Date 2025/9/23
 */
public class DownloadfileConfig {
    private static Logger logger = LoggerFactory.getLogger(DownloadfileConfig.class);
    private FtpConfig ftpConfig;
    private OSSFileInputConfig ossFileInputConfig;
    private JobFileFilter jobFileFilter;
    private boolean scanChild;
    private String fileNameRegular;
    private Pattern fileNameRexPattern = null;
    private String sourcePath;


    /**
     * 是否是文件生命周期管理任务：支持本地文件、Ftp文件以及OSS文件生命周期管理
     * 对于最近修改时间超过指定时长fileLiveTime的文件进行删除处理
     */
    protected boolean lifecycle;

    /**
     * 文件生命周期管理：文件保存时间时长，单位：毫秒
     */
    protected Long fileLiveTime;
    
     


    public Long getFileLiveTime() {
        return fileLiveTime;
    }

    /**
     * 文件生命周期管理：设置文件保存时间时长，单位：毫秒
     * @param fileLiveTime
     * @return
     */
    public DownloadfileConfig setFileLiveTime(long fileLiveTime) {
        this.fileLiveTime = fileLiveTime;
        return this;
    }

    public String getSourcePath() {
        return sourcePath;
    }

    public DownloadfileConfig setSourcePath(String sourcePath) {
        this.sourcePath = sourcePath;
        return this;
    }

    public DownloadfileConfig setLifecycle(boolean lifecycle) {
        this.lifecycle = lifecycle;
        return this;
    }

    public boolean isLifecycle() {
        return lifecycle;
    }

    public FtpConfig getFtpConfig() {
        return ftpConfig;
    }

    public DownloadfileConfig setFtpConfig(FtpConfig ftpConfig) {
        this.ftpConfig = ftpConfig;
        return this;
    }

    public OSSFileInputConfig getOssFileInputConfig() {
        return ossFileInputConfig;
    }

    public DownloadfileConfig setOssFileInputConfig(OSSFileInputConfig ossFileInputConfig) {
        this.ossFileInputConfig = ossFileInputConfig;
        return this;
    }

    public JobFileFilter getJobFileFilter() {
        return jobFileFilter;
    }

    public DownloadfileConfig setJobFileFilter(JobFileFilter jobFileFilter) {
        this.jobFileFilter = jobFileFilter;
        return this;
    }

    public boolean isScanChild() {
        return scanChild;
    }

    public DownloadfileConfig setScanChild(boolean scanChild) {
        this.scanChild = scanChild;
        return this;
    }

    public String getFileNameRegular() {
        return fileNameRegular;
    }

    public DownloadfileConfig setFileNameRegular(String fileNameRegular) {
        this.fileNameRegular = fileNameRegular;
        return this;
    }
    
    public void init() {
        if(ftpConfig != null){
            if(SimpleStringUtil.isEmpty(ftpConfig.getSourcePath()) ){
                if(SimpleStringUtil.isEmpty(this.getSourcePath())) {
                    if(!this.isLifecycle()) {
                        throw new JobFlowException("没有指定下载本地目录");
                    }
                }
                else{
                    ftpConfig.setSourcePath(this.getSourcePath());
                }
            }
        }
        else  if(ossFileInputConfig != null){
            if(SimpleStringUtil.isEmpty(ossFileInputConfig.getSourcePath()) ){
                if(SimpleStringUtil.isEmpty(this.getSourcePath())) {
                    if(!this.isLifecycle()) {
                        throw new JobFlowException("没有指定下载本地目录");
                    }
                }
                else{
                    ossFileInputConfig.setSourcePath(this.getSourcePath());
                }
            }
        }
        else{
            if(SimpleStringUtil.isEmpty(this.getSourcePath())) {
                if(this.isLifecycle()) {
                    throw new JobFlowException("没有指定文件目录");
                }
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
        if(this.isLifecycle()){
            if((this.fileLiveTime == null || this.fileLiveTime <= 0)
                    && jobFileFilter == null){
                throw new JobFlowException("请指定文件保存时长或者指定归档清理文件过滤器");
            }

            if(this.jobFileFilter != null){
                jobFileFilter = new LifecycleJobFileFilter(jobFileFilter,this);
            }
            else{
                jobFileFilter = new LifecycleJobFileFilter(null,this);
            }
        }
        if(ftpConfig != null){
            ftpConfig.initJob(this);

        }
        else if(ossFileInputConfig != null){
            ossFileInputConfig.initJob(this);
        }
        
    }
    
    public void destroy() {
        if(ftpConfig != null){
            ftpConfig.destroy();
        }
        if(ossFileInputConfig != null){
            ossFileInputConfig.destroy();
        }
    }
}
