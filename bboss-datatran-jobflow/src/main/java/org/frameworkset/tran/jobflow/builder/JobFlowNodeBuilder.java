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
import org.frameworkset.tran.jobflow.NodeTrigger;
import org.frameworkset.tran.jobflow.NodeTriggerCreate;
import org.frameworkset.tran.jobflow.listener.JobFlowNodeListener;

import java.util.ArrayList;
import java.util.List;

/**
 * <p>Description: </p>
 * <p></p>
 *
 * @author biaoping.yin
 * @Date 2025/3/31
 */
public abstract class JobFlowNodeBuilder {

    protected JobFlowNodeBuilder parentJobFlowNodeBuilder;
    
    protected JobFlowNodeBuilder nextJobFlowNodeBuilder;
    protected String nodeId;
    protected String nodeName;
   
    protected List<JobFlowNodeListener> jobFlowNodeListeners;


    /**
     * 节点执行触发器构建器
     * 1.串行节点触发条件
     * 2.并行节点类型亦可以设置一个整体的节点触发器
     */
    protected NodeTriggerCreate nodeTriggerCreate;

    /**
     * 节点执行触发器构建器
     * 1.串行节点触发条件
     * 2.并行节点类型亦可以设置一个整体的节点触发器
     */
    protected NodeTrigger nodeTrigger;
    /**
     *  创建的流程作业节点
     */
    protected JobFlowNode jobFlowNode ;
    public JobFlowNodeBuilder(String nodeId,String nodeName){
        this.nodeId = nodeId;
        this.nodeName = nodeName;
    }

    public JobFlowNodeBuilder(){
         
    }

    public JobFlowNodeBuilder addJobFlowNodeListener(JobFlowNodeListener jobFlowNodeListener){
        if(jobFlowNodeListeners == null){
            jobFlowNodeListeners = new ArrayList<>();
        }
        jobFlowNodeListeners.add(jobFlowNodeListener);
        return this;
    }
    protected void setParentJobFlowNodeBuilder(JobFlowNodeBuilder parentJobFlowNodeBuilder) {
        this.parentJobFlowNodeBuilder = parentJobFlowNodeBuilder;
    }
    /**
     * 设置节点的触发器构建器，如果总触发器不成立，如果是复合节点，在不执行nodeBuilders中的所有子任务，每个子任务都可以有自己的触发器，如果简单作业节点则不执行简单作业节点
     * @param nodeTriggerCreate
     * @return
     */
    public JobFlowNodeBuilder setNodeTriggerCreate(NodeTriggerCreate nodeTriggerCreate){
        this.nodeTriggerCreate = nodeTriggerCreate;
        return this;
    }
    public JobFlowNodeBuilder getParentJobFlowNodeBuilder() {
        return parentJobFlowNodeBuilder;
    }

    public JobFlowNode getJobFlowNode() {
        return jobFlowNode;
    }

    public JobFlowNodeBuilder getNextJobFlowNodeBuilder() {
        return nextJobFlowNodeBuilder;
    }
    /**
     * 添加后续节点构建器，如果存在则添加
     * @param nextJobFlowNodeBuilder
     */
    protected void setNextJobFlowNodeBuilder(JobFlowNodeBuilder nextJobFlowNodeBuilder){
        this.nextJobFlowNodeBuilder = nextJobFlowNodeBuilder;
        this.nextJobFlowNodeBuilder.setParentJobFlowNodeBuilder(this);
    }
    
    public abstract JobFlowNode build(JobFlow jobFlow);

    public String getNodeId() {
        return nodeId;
    }

    public JobFlowNodeBuilder setNodeId(String nodeId) {
        this.nodeId = nodeId;
        return this;
    }

    public String getNodeName() {
        return nodeName;
    }

    public JobFlowNodeBuilder setNodeName(String nodeName) {
        this.nodeName = nodeName;
        return this;
    }


    /**
     * 设置节点的触发器构建器，如果总触发器不成立，如果是复合节点，在不执行nodeBuilders中的所有子任务，每个子任务都可以有自己的触发器，如果简单作业节点则不执行简单作业节点
     * @param nodeTrigger
     * @return
     */
    public JobFlowNodeBuilder setNodeTrigger(NodeTrigger nodeTrigger) {
        this.nodeTrigger = nodeTrigger;
        return this;
    }

    public NodeTrigger getNodeTrigger() {
        return nodeTrigger;
    }
}


