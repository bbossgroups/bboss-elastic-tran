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

import org.frameworkset.tran.jobflow.JobFlowNode;

/**
 * 条件复合节点上下文：条件上下复合节点上下文不用于保存容器节点参数数据，因此不能用户节点内部子节点之间传递和共享参数，如需要，则需获取条件复合节点上下文对应的复合节点上下文
 * @author biaoping.yin
 * @Date 2025/3/31
 */
public class ConditionJobFlowNodeExecuteContext extends DefaultJobFlowNodeExecuteContext{    
    private JobFlowNodeExecuteContext matchedJobFlowNodeExecuteContext;
    
    public ConditionJobFlowNodeExecuteContext(JobFlowNode jobFlowNode ){
        
        super(jobFlowNode);
        
    }

    public void setMatchedJobFlowNodeExecuteContext(JobFlowNodeExecuteContext matchedJobFlowNodeExecuteContext) {
        this.matchedJobFlowNodeExecuteContext = matchedJobFlowNodeExecuteContext;
    }

    public JobFlowNodeExecuteContext getMatchedJobFlowNodeExecuteContext() {
        return matchedJobFlowNodeExecuteContext;
    }
}
