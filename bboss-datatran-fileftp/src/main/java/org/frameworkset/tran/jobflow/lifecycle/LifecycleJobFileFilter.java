package org.frameworkset.tran.jobflow.lifecycle;
/**
 * Copyright 2020 bboss
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

import org.frameworkset.tran.input.file.FilterFileInfo;
import org.frameworkset.tran.jobflow.DownloadfileConfig;
import org.frameworkset.tran.jobflow.context.JobFlowNodeExecuteContext;
import org.frameworkset.tran.jobflow.scan.JobFileFilter;

/**
 * 判断是否需要归档文件，如果文件最后修改时间超过fileLiveTime则继续判断其他归档规则，如果归档规则返回true则将文件归档
 * Copyright (c) 2025
 * @Date 2021/8/5 19:46
 * @author biaoping.yin
 * @version 1.0
 */
public class LifecycleJobFileFilter implements JobFileFilter {
    private JobFileFilter jobFileFilter;
    private DownloadfileConfig downloadfileConfig;
    
    public LifecycleJobFileFilter(JobFileFilter jobFileFilter, DownloadfileConfig downloadfileConfig){
        this.jobFileFilter = jobFileFilter;
        this.downloadfileConfig = downloadfileConfig;
    }

	/**
	 * 判断是否需要归档文件，如果文件最后修改时间超过fileLiveTime则继续判断其他归档规则，如果归档规则返回true则将文件归档
	 * @param fileInfo
	 * @param jobFlowNodeExecuteContext
	 * @return
	 */
	public boolean accept(FilterFileInfo fileInfo, JobFlowNodeExecuteContext jobFlowNodeExecuteContext){
        if(!fileInfo.isDirectory()){
            long mtime = fileInfo.getLastModified();
            long currentTime = System.currentTimeMillis();
            if(downloadfileConfig.getFileLiveTime() != null) {
                if (currentTime - mtime >= downloadfileConfig.getFileLiveTime()) {
                    if (jobFileFilter == null) {
                        return true;
                    }
                    return jobFileFilter.accept(fileInfo, jobFlowNodeExecuteContext);
                } else {
                    return false;
                }
            }
            else{
                if (jobFileFilter == null) {
                    return false;
                }
                return jobFileFilter.accept(fileInfo, jobFlowNodeExecuteContext);
            }
        }
        else {
            if(jobFileFilter == null){
                return true;
            }
            return jobFileFilter.accept(fileInfo, jobFlowNodeExecuteContext);
        }
    }
}
