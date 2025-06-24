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
import org.frameworkset.tran.jobflow.metrics.JobFlowMetrics;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * <p>Description: 流程执行上下文对象，用于在流程子任务之间传递参数，每次流程调度开始之前进行初始化或者重置</p>
 * <p></p>
 *
 * @author biaoping.yin
 * @Date 2025/4/6
 */
public class DefaultJobFlowExecuteContext implements JobFlowExecuteContext {
    private Map<String,Object> contextDatas = new LinkedHashMap<>();
    private JobFlow jobFlow;
    public DefaultJobFlowExecuteContext(JobFlow jobFlow){
        this.jobFlow = jobFlow;
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
    public JobFlowMetrics getJobFlowMetrics() {
        return jobFlow.getJobFlowMetrics();
    }

    @Override
    public StaticContext getJobFlowStaticContext() {
        return jobFlow.getJobFlowContext().copy();
    }
    
    
}
