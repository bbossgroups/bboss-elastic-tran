package org.frameworkset.tran.jobflow.listener;
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

import org.frameworkset.tran.jobflow.JobFlowNode;
import org.frameworkset.tran.jobflow.context.JobFlowNodeExecuteContext;

/**
 * 工作流节点执行拦截器
 * @author biaoping.yin
 * @Date 2025/6/17
 */
public interface JobFlowNodeListener {

 
    /**
     * 作业工作流节点调度执行前拦截方法
     * @param jobFlowNodeExecuteContext
     */
    void beforeExecute(JobFlowNodeExecuteContext jobFlowNodeExecuteContext);

    /**
     * 作业工作流节点调度执行完毕后执行方法
     * @param jobFlowNodeExecuteContext
     */
    void afterExecute(JobFlowNodeExecuteContext jobFlowNodeExecuteContext,Throwable throwable);

    /**
     * 作业工作流节点结束时拦截方法
     * @param jobFlowNode
     */
    void afterEnd(JobFlowNode jobFlowNode);
}
