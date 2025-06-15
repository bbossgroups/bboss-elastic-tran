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

import org.frameworkset.tran.config.ImportBuilder;

import java.util.ArrayList;
import java.util.List;

/**
 * 
 * @author biaoping.yin
 * @Date 2025/3/31
 */
public class CompositionJobFlowNodeBuilder extends JobFlowNodeBuilder{
    
     
    private JobFlowNodeType jobFlowNodeType ; 
    /**
     * 并行节点作业配置
     */
    private List<JobFlowNodeBuilder> nodeBuilders;
    
    public CompositionJobFlowNodeBuilder(JobFlowNodeType jobFlowNodeType){
        this.jobFlowNodeType = jobFlowNodeType;
    }

    public CompositionJobFlowNodeBuilder(){
        jobFlowNodeType = JobFlowNodeType.SEQUENCE;
    }



    private void init(){
        if(nodeBuilders == null){
            nodeBuilders = new ArrayList<>();
        }
    }


    /**
     * 设置复合节点的总触发器构建器，如果总触发器不成立，在不执行nodeBuilders中的所有子任务，每个子任务都可以有自己的触发器
     * @param compositionNodeTriggerCreate
     * @return
     */
    public CompositionJobFlowNodeBuilder addNodeTriggerCreate(NodeTriggerCreate compositionNodeTriggerCreate){
        this.nodeTriggerCreate = compositionNodeTriggerCreate;
        return this;
    }
    /**
     * 添加一个简单的子任务,并设置任务触发器
     * @param importBuilderCreate
     * @param nodeTriggerCreate
     * @return
     */
    public CompositionJobFlowNodeBuilder addImportBuilder(ImportBuilderCreate importBuilderCreate, NodeTriggerCreate nodeTriggerCreate){
//        this.nodeBuilder = importBuilderCreate.createImportBuilder(this);
        init();
        nodeBuilders.add(new SimpleJobFlowNodeBuilder().buildImportBuilder(importBuilderCreate,nodeTriggerCreate));
        return this;
    }

    public CompositionJobFlowNodeBuilder addImportBuilder(ImportBuilder importBuilder, NodeTrigger nodeTrigger){
//        this.nodeBuilder = importBuilderCreate.createImportBuilder(this);
        init();
        nodeBuilders.add(new SimpleJobFlowNodeBuilder().setImportBuilder(importBuilder).setNodeTrigger(nodeTrigger));
        return this;
    }

    /**
     * 添加一个简单的子任务
     * @param importBuilderCreate
     * @return
     */
    public CompositionJobFlowNodeBuilder addImportBuilder(ImportBuilderCreate importBuilderCreate){
//        this.nodeBuilder = importBuilderCreate.createImportBuilder(this);
        init();
        nodeBuilders.add(new SimpleJobFlowNodeBuilder().buildImportBuilder(importBuilderCreate));
        return this;
    }

    /**
     * 添加一个简单的子任务
     * @param importBuilder
     * @return
     */
    public CompositionJobFlowNodeBuilder addImportBuilder(ImportBuilder importBuilder){
//        this.nodeBuilder = importBuilderCreate.createImportBuilder(this);
        init();
        nodeBuilders.add(new SimpleJobFlowNodeBuilder().setImportBuilder(importBuilder));
        return this;
    }

    /**
     * 添加复杂并行子任务，存在串行多个子任务或者串行任务
     * @param jobFlowNodeBuilder
     * @return
     */
    public CompositionJobFlowNodeBuilder addJobFlowNodeBuilder(CompositionJobFlowNodeBuilder jobFlowNodeBuilder){
//        this.nodeBuilder = importBuilderCreate.createImportBuilder(this);
        init();
        nodeBuilders.add(jobFlowNodeBuilder);
        return this;
    }

    @Override
    public JobFlowNode build(JobFlow jobFlow){
        CompositionJobFlowNode compositionJobFlowNode = null;
        if(jobFlowNodeType == JobFlowNodeType.SEQUENCE) {
            compositionJobFlowNode = new SequenceJobFlowNode();
        }
        else {
            compositionJobFlowNode = new ParrelJobFlowNode();
        }
        compositionJobFlowNode.setNodeId(this.getNodeId());
        compositionJobFlowNode.setNodeName(this.getNodeName());
        compositionJobFlowNode.setJobFlow(jobFlow);
        if(this.parentJobFlowNodeBuilder != null) {
            compositionJobFlowNode.setParentJobFlowNode(parentJobFlowNodeBuilder.getJobFlowNode());
        }
        if(this.nodeTriggerCreate != null){
            compositionJobFlowNode.setNodeTrigger(this.nodeTriggerCreate.createNodeTrigger(this));
        }
        for(JobFlowNodeBuilder jobFlowNodeBuilder:nodeBuilders){
            compositionJobFlowNode.addJobFlowNode(jobFlowNodeBuilder.build(jobFlow));
        }
        this.jobFlowNode = compositionJobFlowNode;
        if(this.nextJobFlowNodeBuilder != null){
            JobFlowNode nextJobFlowNode = nextJobFlowNodeBuilder.build(jobFlow);
            this.jobFlowNode.setNextJobFlowNode(nextJobFlowNode);
        }
        return compositionJobFlowNode;

    }

    public CompositionJobFlowNodeBuilder setJobFlowNodeType(JobFlowNodeType jobFlowNodeType) {
        if(jobFlowNodeType == JobFlowNodeType.SIMPLE){
            throw new JobFlowException("CompositionJobFlowNodeBuilder jobFlowNodeType must be set SEQUENCE or PARREL,but is SIMPLE.");
        }
        this.jobFlowNodeType = jobFlowNodeType;
        return this;
    }

    public JobFlowNodeType getJobFlowNodeType() {
        return jobFlowNodeType;
    }
}
