package org.frameworkset.tran.jobflow.context;
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

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * <p>Description: </p>
 * <p></p>
 *
 * @author biaoping.yin
 * @Date 2025/3/31
 */
public class DefaultJobFlowNodeExecuteContext implements JobFlowNodeExecuteContext{
    private Map<String,Object> contextDatas = new LinkedHashMap<>();
    private JobFlowNode jobFlowNode;
    private JobFlow jobFlow;
    
    public DefaultJobFlowNodeExecuteContext(JobFlowNode jobFlowNode){
        this.jobFlowNode = jobFlowNode;
        this.jobFlow = jobFlowNode.getJobFlow();
        
    }
    @Override
    public synchronized Object getContextData(String name) {
        return contextDatas.get(name);
    }


    @Override
    public synchronized void putAll(Map<String,Object> contextDatas){
        this.contextDatas.putAll(contextDatas);
    }

    @Override
    public synchronized void addContextData(String name, Object data) {
        this.contextDatas.put(name,data);
    }

    @Override
    public synchronized void clear(){
        this.contextDatas.clear();
    }

    @Override
    public JobFlowExecuteContext getJobFlowExecuteContext() {
        return this.jobFlow.getJobFlowExecuteContext();
    }
    @Override
    public void pauseAwait(){
        this.jobFlow.getJobFlowContext().pauseAwait(jobFlowNode);
    }
    /**
     * 判断作业是否已经停止或者正在停止中
     * @return
     */
    public AssertResult assertStopped(){
        return this.jobFlow.getJobFlowContext().assertStopped();
    }
    

}
