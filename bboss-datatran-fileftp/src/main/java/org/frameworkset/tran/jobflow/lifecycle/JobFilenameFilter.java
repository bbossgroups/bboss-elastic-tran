package org.frameworkset.tran.jobflow.lifecycle;
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

import org.frameworkset.tran.input.file.LocalFilterFileInfo;
import org.frameworkset.tran.jobflow.context.JobFlowNodeExecuteContext;
import org.frameworkset.tran.jobflow.scan.JobFileFilter;

import java.io.File;
import java.io.FilenameFilter;

/**
 * 本地文件归档过滤器：筛选已经过期的需归档文件
 * @author biaoping.yin
 * @Date 2025/9/25
 */
public class JobFilenameFilter implements FilenameFilter {

    private JobFileFilter jobFileFilter;
    private JobFlowNodeExecuteContext jobFlowNodeExecuteContext;
    public JobFilenameFilter(JobFileFilter jobFileFilter, JobFlowNodeExecuteContext jobFlowNodeExecuteContext){
        this.jobFileFilter = jobFileFilter;
        this.jobFlowNodeExecuteContext = jobFlowNodeExecuteContext;
    }
    /**
     * Tests if a specified file should be included in a file list.
     *
     * @param dir  the directory in which the file was found.
     * @param name the name of the file.
     * @return {@code true} if and only if the name should be
     * included in the file list; {@code false} otherwise.
     */
    @Override
    public boolean accept(File dir, String name) {
        return jobFileFilter.accept(new LocalFilterFileInfo(dir, name), jobFlowNodeExecuteContext);
    }
}
