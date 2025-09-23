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
import org.frameworkset.tran.jobflow.context.JobFlowNodeExecuteContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 记录已经下载文件记录到日志文件中，默认实现方式，用户可以自行实现自己的记录下载
 * 
 * @author biaoping.yin
 * @Date 2025/9/22
 */
public class DefaultDownloadedFileRecorder implements DownloadedFileRecorder {
    private Logger logger = LoggerFactory.getLogger(DefaultDownloadedFileRecorder.class);

    /**
     * 通过本方法记录下载文件信息，同时亦可以判断文件是否已经下载过，如果已经下载过则返回false，忽略下载，否则返回true允许下载
     *
     * @param downloadFileMetrics
     * @param jobFlowNodeExecuteContext
     */
    @Override
    public boolean recordBeforeDownload(DownloadFileMetrics downloadFileMetrics ,  JobFlowNodeExecuteContext jobFlowNodeExecuteContext) {
        logger.info("Record before download remoteFilePath:{},localFilePath:{},nodeId:{},nodeName:{}",
                downloadFileMetrics.getRemoteFilePath(), downloadFileMetrics.getLocalFilePath(), jobFlowNodeExecuteContext.getNodeId(), jobFlowNodeExecuteContext.getNodeName());
        return true;
    }

    /**
     * 通过本方法记录下载文件信息，同时亦可以判断文件是否已经下载过，如果已经下载过则返回false，忽略下载，否则返回true允许下载
     *
     * @param downloadFileMetrics
     * @param jobFlowNodeExecuteContext
     */
    public void recordAfterDownload(DownloadFileMetrics downloadFileMetrics , JobFlowNodeExecuteContext jobFlowNodeExecuteContext, Throwable exception){
        if(exception == null) {
            logger.info("Record after complete download:{},nodeId:{},nodeName:{}",
                    SimpleStringUtil.object2json(downloadFileMetrics), jobFlowNodeExecuteContext.getNodeId(), jobFlowNodeExecuteContext.getNodeName());
        }
        else{
            logger.warn("Record after exception download:{},localFilePath:{},nodeId:{},nodeName:{}",
                    SimpleStringUtil.object2json(downloadFileMetrics), jobFlowNodeExecuteContext.getNodeId(), jobFlowNodeExecuteContext.getNodeName());
            logger.warn("",exception);
        }
    }

}
