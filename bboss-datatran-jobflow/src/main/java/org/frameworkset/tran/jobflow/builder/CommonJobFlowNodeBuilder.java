package org.frameworkset.tran.jobflow.builder;
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

import org.frameworkset.tran.jobflow.JobFlow;
import org.frameworkset.tran.jobflow.JobFlowNode;
import org.frameworkset.tran.jobflow.JobFlowNodeFunction;
import org.frameworkset.tran.jobflow.SimpleJobFlowNode;

/**
 * 通用工作流作业节点构建器
 * @author biaoping.yin
 * @Date 2025/3/31
 */
public class CommonJobFlowNodeBuilder extends SimpleJobFlowNodeBuilder {
    

    protected JobFlowNodeFunction jobFlowNodeFunction;
    /**
     * 串行节点作业配置
     */
//    protected ImportBuilder importBuilder;

    public CommonJobFlowNodeBuilder(String nodeId, String nodeName,JobFlowNodeFunction jobFlowNodeFunction) {
        super(nodeId, nodeName);        
        this.jobFlowNodeFunction = jobFlowNodeFunction;
    }

    public CommonJobFlowNodeBuilder setAutoNodeComplete(boolean autoNodeComplete) {
        this.autoNodeComplete = autoNodeComplete;
        return this;
    }

    
    protected  JobFlowNodeFunction buildJobFlowNodeFunction(){
   
        return jobFlowNodeFunction;
    }




}
