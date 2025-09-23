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

    public JobFileFilter getFileFilter() {
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
