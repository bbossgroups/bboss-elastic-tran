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

import org.frameworkset.tran.jobflow.context.JobFlowNodeExecuteContext;

/**
 * 文件下载情况记录
 * @author biaoping.yin
 * @Date 2025/9/22
 */
public interface DownloadedFileRecorder {

    /**
     * 通过本方法记录下载文件信息，同时亦可以判断文件是否已经下载过，如果已经下载过则返回false，忽略下载，否则返回true允许下载
     * @param downloadFileMetrics
     * @param jobFlowNodeExecuteContext
     * @return 根据返回值控制是否继续下载文件，true:下载，false:停止下载
     */
    boolean recordBeforeDownload(DownloadFileMetrics downloadFileMetrics , JobFlowNodeExecuteContext jobFlowNodeExecuteContext);
    /**
     * 文件下载完毕或者错误时调用本方法记录已完成或者下载失败文件信息，可以自行存储下载文件信息，以便下次下载时判断文件是否已经下载过
     * @param downloadFileMetrics
     * @param jobFlowNodeExecuteContext
     * @param exception
     */
    void recordAfterDownload(DownloadFileMetrics downloadFileMetrics , JobFlowNodeExecuteContext jobFlowNodeExecuteContext,Throwable exception);

}
