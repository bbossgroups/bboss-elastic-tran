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


import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.schedule.TaskContext;

import java.util.List;

/**
 * <p>Description: 作业任务编排流程</p>
 * <p></p>
 *
 * @author biaoping.yin
 * @Date 2025/3/31
 */
public class JobFlow {
    /**
     * 作业流程id
     */
    private String jobFlowId;
    /**
     * 作业流程名称
     */
    private String jobFlowName;
    /**
     * 流程的首节点
     */
    private JobFlowNode startJobFlowNode;
    
    private List<ComJobFlowNode> jobFlowNodes;

    private JobFlowExecuteContext jobFlowExecuteContext;

    public void setStartJobFlowNode(JobFlowNode startJobFlowNode) {
        this.startJobFlowNode = startJobFlowNode;
    }

    /**
     * 启动工作流
     */
    public void start(){
        JobFlowExecuteContext jobFlowExecuteContext = new DefaultJobFlowExecuteContext() ;
        this.jobFlowExecuteContext = jobFlowExecuteContext;
        this.startJobFlowNode.start();
    }

    /**
     * 停止工作流
     */
    public void stop(){
        this.startJobFlowNode.stop();
    }

    /**
     * 暂停工作流
     */
    public void pause(){
        this.startJobFlowNode.pause();
    }

    /**
     * 唤醒工作流
     */
    public void consume(){
        this.startJobFlowNode.consume();
    }

    public JobFlowExecuteContext getJobFlowExecuteContext() {
        return jobFlowExecuteContext;
    }

    public String getJobFlowId() {
        return jobFlowId;
    }

    public void setJobFlowId(String jobFlowId) {
        this.jobFlowId = jobFlowId;
    }

    public String getJobFlowName() {
        return jobFlowName;
    }

    public void setJobFlowName(String jobFlowName) {
        this.jobFlowName = jobFlowName;
    }

    public void setJobFlowExecuteContext(JobFlowExecuteContext jobFlowExecuteContext) {
        this.jobFlowExecuteContext = jobFlowExecuteContext;
    }

    /**
     * 作业结束时触发工作流任务结束回调方法，等待下一次任务的调度，如果是一次性任务，则直接结束流程任务
     */
    public void complete(ImportContext importContext, Throwable e) {
        this.jobFlowExecuteContext.increamentNums();
        this.jobFlowExecuteContext.clear();
    }

    /**
     * 每次作业调度任务迭代结束时触发流程结束回调方法：等待下一次任务的调度，如果是一次性任务，则直接结束流程任务
     */
    public void complete(TaskContext taskContext, Throwable e) {
        this.jobFlowExecuteContext.increamentNums();
        this.jobFlowExecuteContext.clear();
    }
}
