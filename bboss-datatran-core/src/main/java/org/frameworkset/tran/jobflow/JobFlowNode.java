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

/**
 *
 * @author biaoping.yin
 * @Date 2025/3/31
 */
public abstract class JobFlowNode {
    
    private JobFlowNodeType jobFlowNodeType = JobFlowNodeType.SIMPLE;
    protected String nodeId;
    protected String nodeName;
    protected JobFlow jobFlow;
    protected CompositionJobFlowNode compositionJobFlowNode;
    /**
     * 串行节点触发条件
     */
    protected NodeTrigger nodeTrigger;
    /**
     * 父节点作业配置
     * 父节点可能是串行节点，也可能是并行节点
     */
    protected JobFlowNode parentJobFlowNode;

    /**
     * 下一节点作业配置
     * 下一节点可能是串行节点，也可能是并行节点
     */
    protected JobFlowNode nextJobFlowNode;

    public void setCompositionJobFlowNode(CompositionJobFlowNode compositionJobFlowNode) {
        this.compositionJobFlowNode = compositionJobFlowNode;
    }

    public CompositionJobFlowNode getCompositionJobFlowNode() {
        return compositionJobFlowNode;
    }

    public void setParentJobFlowNode(JobFlowNode parentJobFlowNode) {
        this.parentJobFlowNode = parentJobFlowNode;
    }
    
    public boolean assertTrigger(){
        if(nodeTrigger == null){
            return true;
        }
        try {
            if(this.nodeTrigger.assertTrigger(jobFlow)){
                return true;
            }
            else{
                return false;
            }
        } catch (Exception e) {
            throw new JobFlowException("AassertTrigger failed:",e);
        }
    }
    
    

    public JobFlowNode getParentJobFlowNode() {
        return parentJobFlowNode;
    }

    public void setNextJobFlowNode(JobFlowNode nextJobFlowNode) {
        this.nextJobFlowNode = nextJobFlowNode;
    }

    public JobFlowNode getNextJobFlowNode() {
        return nextJobFlowNode;
    }

    public void setNodeTrigger(NodeTrigger nodeTrigger) {
        this.nodeTrigger = nodeTrigger;
    }

    public NodeTrigger getNodeTrigger() {
        return nodeTrigger;
    }

    public void setJobFlow(JobFlow jobFlow) {
        this.jobFlow = jobFlow;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public void setNodeName(String nodeName) {
        this.nodeName = nodeName;
    }

    public String getNodeId() {
        return nodeId;
    }

    public String getNodeName() {
        return nodeName;
    }

    public JobFlow getJobFlow() {
        return jobFlow;
    }

    /**
     * 启动流程当前节点
     */
    public abstract boolean start();
    /**
     * 停止流程当前节点
     */
    public abstract void stop();

    /**
     * 暂停流程节点
     */
    public abstract void pause();

    /**
     * 唤醒暂停流程节点
     */
    public abstract void consume();

    /**
     * 作业结束时，节点任务结束,可以唤醒下一个任务
     * 如果没有下一个任务，则检查是否有父节点：
     * 如果有父节点则反向通知父节点，当前节点已经完成任务,可以采取下一步的措施
     * 如果没有父节点，则可能已经到达工作流的第一个节点，也可能到达并行节点的分支起点
     */
    public void nodeComplete(ImportContext importContext, Throwable e){
        if(this.nextJobFlowNode != null){
            this.nextJobFlowNode.start();
        }
        else{
            if(parentJobFlowNode != null){
                parentJobFlowNode.nextNodeComplete(  importContext,   e);
            }
            else{
                if(this.compositionJobFlowNode != null){
                    compositionJobFlowNode.brachComplete(this,  importContext,   e);
                }
                else{
                    this.jobFlow.complete(  importContext,   e);
                }
            }
        }
    }

    /**
     * 作业结束时，通知父节点，当前节点任务执行结束
     */
    public void nextNodeComplete(ImportContext importContext, Throwable e){
        if(this.parentJobFlowNode != null){
            parentJobFlowNode.nextNodeComplete(  importContext,   e);
        }
        else{
            if(this.compositionJobFlowNode != null){
                this.compositionJobFlowNode.brachComplete(this,  importContext,   e);
            }
            else{
                this.jobFlow.complete(  importContext,   e);
            }
        }
        
    }


    /**
     * 每次作业调度任务迭代结束时触发，节点任务结束,可以唤醒下一个任务
     * 如果没有下一个任务，则检查是否有父节点：
     * 如果有父节点则反向通知父节点，当前节点已经完成任务,可以采取下一步的措施
     * 如果没有父节点，则可能已经到达工作流的第一个节点，也可能到达并行节点的分支起点
     */
    public void nodeComplete(TaskContext taskContext, Throwable e){
        if(this.nextJobFlowNode != null){
            this.nextJobFlowNode.start();
        }
        else{
            if(parentJobFlowNode != null){
                parentJobFlowNode.nextNodeComplete(  taskContext,   e);
            }
            else{
                if(this.compositionJobFlowNode != null){
                    compositionJobFlowNode.brachComplete(this,  taskContext,   e);
                }
                else{
                    this.jobFlow.complete(  taskContext,   e);
                }
            }
        }
    }

    /**
     * 每次作业调度任务迭代结束时触发，通知父节点，当前节点任务执行结束
     */
    public void nextNodeComplete(TaskContext taskContext, Throwable e){
        if(this.parentJobFlowNode != null){
            parentJobFlowNode.nextNodeComplete(  taskContext,   e);
        }
        else{
            if(this.compositionJobFlowNode != null){
                this.compositionJobFlowNode.brachComplete(this,  taskContext,   e);
            }
            else{
                this.jobFlow.complete(  taskContext,   e);
            }
        }

    }

    public void setJobFlowNodeType(JobFlowNodeType jobFlowNodeType) {
        this.jobFlowNodeType = jobFlowNodeType;
    }

    public JobFlowNodeType getJobFlowNodeType() {
        return jobFlowNodeType;
    }
}
