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
 * 工作流执行器函数，一个节点只保存JobFlowNodeFunction的一个实例，因此一次调度执行完毕后，需要通过reset重置状态，release释放资源
 * @author biaoping.yin
 * @Date 2025/6/22
 */
public interface JobFlowNodeFunction {

    /**
     * 初始化构建节点函数实例时，只在构建工作流节点时调用一次
     * @param jobFlowNode
     */
    void init(JobFlowNode jobFlowNode);

    /**
     * 执行工作流函数
     * @param jobFlowNodeExecuteContext
     * @return
     */
    Object call(JobFlowNodeExecuteContext jobFlowNodeExecuteContext) throws Exception;

    /**
     * 重置一些监控状态
     */
    void reset();

    /**
     * 释放资源
     */
    void release();

    /**
     * 暂停
     */
    default void pauseSchedule(){
        
    }

    /**
     * 恢复
     */
    default void resumeSchedule(){
        
    }

    /**
     * 停止
     */
    void stop();
}
