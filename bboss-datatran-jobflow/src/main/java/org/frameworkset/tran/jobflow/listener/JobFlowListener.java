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

import org.frameworkset.tran.jobflow.JobFlow;
import org.frameworkset.tran.jobflow.context.JobFlowExecuteContext;

/**
 * 工作流执行拦截器
 * @author biaoping.yin
 * @Date 2025/6/17
 */
public interface JobFlowListener {
    /**
     * 作业工作流开始前拦截方法
     * @param jobFlow
     */
    void beforeStart(JobFlow jobFlow);

    /**
     * 作业工作流调度执行前拦截方法
     * @param jobFlowExecuteContext
     */
    void beforeExecute(JobFlowExecuteContext jobFlowExecuteContext);

    /**
     * 作业工作流调度执行完毕后执行方法
     * @param jobFlowExecuteContext
     */
    void afterExecute(JobFlowExecuteContext jobFlowExecuteContext,Throwable throwable);

    /**
     * 作业工作流完成停止时拦截方法
     * @param jobFlow
     */
    void afterEnd(JobFlow jobFlow);
    

}
