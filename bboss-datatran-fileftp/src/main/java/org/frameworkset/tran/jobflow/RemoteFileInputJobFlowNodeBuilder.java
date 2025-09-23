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

import org.frameworkset.tran.jobflow.builder.SimpleJobFlowNodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件采集节点构建器，继承SimpleJobFlowNodeBuilder
 * 
 * @author biaoping.yin
 * @Date 2025/9/17
 */
public class RemoteFileInputJobFlowNodeBuilder extends SimpleJobFlowNodeBuilder<RemoteFileInputJobFlowNodeBuilder> {

    private static Logger logger = LoggerFactory.getLogger(RemoteFileInputJobFlowNodeBuilder.class);
  
    private BuildDownloadConfigFunction buildDownloadConfigFunction;
    private DownloadfileConfig downloadfileConfig;
    
    private DownloadedFileRecorder downloadedFileRecorder;
 
   
 
    /**
     * 文件采集作业节点构建函数
     *
     * @param nodeId
     * @param nodeName
     */
    public RemoteFileInputJobFlowNodeBuilder(String nodeId, String nodeName) {
        super(nodeId, nodeName);
        this.setAutoNodeComplete(true);
    }

    
    public RemoteFileInputJobFlowNodeBuilder(){
        super();
        this.setAutoNodeComplete(true);
    }
    /**
     * 构建文件采集函数
     * @return
     */
    @Override
    protected JobFlowNodeFunction buildJobFlowNodeFunction() {
        
        DownloadJobFlowNodeFunction downloadJobFlowNodeFunction = new DownloadJobFlowNodeFunction();
        downloadJobFlowNodeFunction.setBuildDownloadConfigFunction(buildDownloadConfigFunction);
        downloadJobFlowNodeFunction.setDownloadedFileRecord(downloadedFileRecorder);
        downloadJobFlowNodeFunction.setDownloadfileConfig(downloadfileConfig);
        return downloadJobFlowNodeFunction;
    }


    public RemoteFileInputJobFlowNodeBuilder setBuildDownloadConfigFunction(BuildDownloadConfigFunction buildDownloadConfigFunction) {
        this.buildDownloadConfigFunction = buildDownloadConfigFunction;
        return this;
    }

    public RemoteFileInputJobFlowNodeBuilder setDownloadfileConfig(DownloadfileConfig downloadfileConfig) {
        this.downloadfileConfig = downloadfileConfig;
        return this;
    }
    
    public RemoteFileInputJobFlowNodeBuilder setDownloadedFileRecorder(DownloadedFileRecorder downloadedFileRecorder) {
        this.downloadedFileRecorder = downloadedFileRecorder;
        return this;
    }
}
