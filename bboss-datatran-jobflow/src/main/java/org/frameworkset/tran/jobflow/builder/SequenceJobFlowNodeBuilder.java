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
import org.frameworkset.tran.jobflow.JobFlowNodeType;
import org.frameworkset.tran.jobflow.SequenceJobFlowNode;

/**
 * 
 * @author biaoping.yin
 * @Date 2025/3/31
 */
public class SequenceJobFlowNodeBuilder extends CompositionJobFlowNodeBuilder<SequenceJobFlowNodeBuilder> {

    private JobFlowNodeBuilder headerJobFlowNodeBuilder;
    private JobFlowNodeBuilder currentJobFlowNodeBuilder;

   
    public SequenceJobFlowNodeBuilder(String nodeId,String nodeName){
        super(nodeId,nodeName,JobFlowNodeType.SEQUENCE);
    }

    public SequenceJobFlowNodeBuilder(){
        super(JobFlowNodeType.SEQUENCE);
    }


    /**
     * 添加复杂串行子任务，存在串行多个子任务或者串行任务
     * @param jobFlowNodeBuilder
     * @return
     */
    public SequenceJobFlowNodeBuilder addJobFlowNodeBuilder(JobFlowNodeBuilder jobFlowNodeBuilder){
        init();
        validate(jobFlowNodeBuilder);
        jobFlowNodeBuilder.setCompositionJobFlowNodeBuilder(this);
        if(this.headerJobFlowNodeBuilder == null) {
            this.headerJobFlowNodeBuilder = jobFlowNodeBuilder;

        }
        if(currentJobFlowNodeBuilder != null)
            this.currentJobFlowNodeBuilder.setNextJobFlowNodeBuilder(jobFlowNodeBuilder);
        this.currentJobFlowNodeBuilder = jobFlowNodeBuilder;
        nodeBuilders.add(jobFlowNodeBuilder);
        return this;
    }
    
    /**
     * 串行流程节点管理：为当前作业节点添加带条件的下一个作业节点
     * @param jobFlowNodeBuilder
     * @return
     */
    public SequenceJobFlowNodeBuilder addConditionJobFlowNodeBuilder(JobFlowNodeBuilder jobFlowNodeBuilder){
        return addConditionJobFlowNodeBuilder(jobFlowNodeBuilder,false);
    }
    /**
     * 串行流程节点管理：为当前作业节点添加带条件的下一个作业节点
     * @param jobFlowNodeBuilder
     * @param defaultConditionNode 是否默认条件节点,条件节点必须配置一个默认流程节点
     * @return
     */
    public SequenceJobFlowNodeBuilder addConditionJobFlowNodeBuilder(JobFlowNodeBuilder jobFlowNodeBuilder,boolean defaultConditionNode){
        init();
        CompositionJobFlowNodeBuilder compositionJobFlowNodeBuilder = jobFlowNodeBuilder.getCompositionJobFlowNodeBuilder();
        if(compositionJobFlowNodeBuilder != null ){
            if(compositionJobFlowNodeBuilder != this) {
                throw new JobFlowBuilderException("条件节点只能添加到当前复合节点中");
            }
        }
        else{
            jobFlowNodeBuilder.setCompositionJobFlowNodeBuilder(this);
        }
        jobFlowNodeBuilder.setDefaultConditionNode(defaultConditionNode);
        if(currentJobFlowNodeBuilder != null) {
            if(currentJobFlowNodeBuilder instanceof ConditionJobFlowNodeBuilder){
                ((ConditionJobFlowNodeBuilder)currentJobFlowNodeBuilder).addJobFlowNodeBuilder(jobFlowNodeBuilder);
            }
            else {
                ConditionJobFlowNodeBuilder conditionJobFlowNodeBuilder = (ConditionJobFlowNodeBuilder) currentJobFlowNodeBuilder.getNextJobFlowNodeBuilder();
                if (conditionJobFlowNodeBuilder != null) {
                    conditionJobFlowNodeBuilder.addJobFlowNodeBuilder(jobFlowNodeBuilder);
                } else {
                    conditionJobFlowNodeBuilder = new ConditionJobFlowNodeBuilder();
                    conditionJobFlowNodeBuilder.addJobFlowNodeBuilder(jobFlowNodeBuilder);
                    currentJobFlowNodeBuilder.setNextJobFlowNodeBuilder(conditionJobFlowNodeBuilder);
                    nodeBuilders.add(conditionJobFlowNodeBuilder);
                }
            }

        }
        else{
            ConditionJobFlowNodeBuilder conditionJobFlowNodeBuilder = new ConditionJobFlowNodeBuilder();
            conditionJobFlowNodeBuilder.addJobFlowNodeBuilder(jobFlowNodeBuilder);
            this.currentJobFlowNodeBuilder = conditionJobFlowNodeBuilder;
            nodeBuilders.add(conditionJobFlowNodeBuilder);
            if(this.headerJobFlowNodeBuilder == null) {
                this.headerJobFlowNodeBuilder = conditionJobFlowNodeBuilder;

            }
        }
        return this;
    }


    @Override
    public JobFlowNode build(JobFlow jobFlow){
        if(this.jobFlowNode != null){
            return jobFlowNode;
        }
        SequenceJobFlowNode sequenceJobFlowNode = new SequenceJobFlowNode();
         
        sequenceJobFlowNode.setNodeId(this.getNodeId());
        sequenceJobFlowNode.setNodeName(this.getNodeName());
        sequenceJobFlowNode.setJobFlow(jobFlow);
        if(this.parentJobFlowNodeBuilder != null) {
            sequenceJobFlowNode.setParentJobFlowNode(parentJobFlowNodeBuilder.getJobFlowNode());
        }
        if(this.nodeTrigger != null){
            sequenceJobFlowNode.setNodeTrigger(nodeTrigger);
        }
        else if(this.nodeTriggerCreate != null){
            sequenceJobFlowNode.setNodeTrigger(this.nodeTriggerCreate.createNodeTrigger(this));
        }
        //构建顺序节点链路
        sequenceJobFlowNode.setHeaderJobFlowNode(headerJobFlowNodeBuilder.build(jobFlow));
 
        this.jobFlowNode = sequenceJobFlowNode;
        if(this.nextJobFlowNodeBuilder != null){
            JobFlowNode nextJobFlowNode = nextJobFlowNodeBuilder.build(jobFlow);
            this.jobFlowNode.setNextJobFlowNode(nextJobFlowNode);
        }

        sequenceJobFlowNode.setJobFlowNodeListeners(this.jobFlowNodeListeners);
        return sequenceJobFlowNode;

    }


}
