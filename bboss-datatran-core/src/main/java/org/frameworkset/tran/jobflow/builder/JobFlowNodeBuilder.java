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

import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.jobflow.JobFlow;
import org.frameworkset.tran.jobflow.JobFlowNode;
import org.frameworkset.tran.jobflow.NodeTrigger;
import org.frameworkset.tran.jobflow.NodeTriggerCreate;

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
    /**
     * 串行节点作业配置生成接口
     */
    protected ImportBuilderCreate importBuilderCreate;

    /**
     * 串行节点作业配置
     */
    protected ImportBuilder importBuilder;



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

    public void setParentJobFlowNodeBuilder(JobFlowNodeBuilder parentJobFlowNodeBuilder) {
        this.parentJobFlowNodeBuilder = parentJobFlowNodeBuilder;
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
    public void setNextJobFlowNodeBuilder(JobFlowNodeBuilder nextJobFlowNodeBuilder){
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

    public JobFlowNodeBuilder setImportBuilder(ImportBuilder importBuilder) {
        this.importBuilder = importBuilder;
        return this;
    }

    public ImportBuilder getImportBuilder() {
        return importBuilder;
    }

    public JobFlowNodeBuilder setNodeTrigger(NodeTrigger nodeTrigger) {
        this.nodeTrigger = nodeTrigger;
        return this;
    }

    public NodeTrigger getNodeTrigger() {
        return nodeTrigger;
    }
}


