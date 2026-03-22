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

import com.frameworkset.util.SimpleStringUtil;
import org.frameworkset.tran.jobflow.*;


/**
 * 条件作业节点工作流作业节点
 * @author biaoping.yin
 * @Date 2025/3/31
 */
public class ConditionJobFlowNodeBuilder extends CompositionJobFlowNodeBuilder<ConditionJobFlowNodeBuilder> {
    private String conditionJobFlowNodeUUID;
    /**
     * 第一次执行条件节点时，所有节点条件都不匹配时，是否继续执行条件节点后续节点
     * true 执行，false 终止执行，默认值false
     * 需要在添加第一个条件节点时时，进行设置
     */
    private boolean allCondtionNodeMathfailedContinue;
    public ConditionJobFlowNodeBuilder(){
        super(JobFlowNodeType.CONDITION);
        this.conditionJobFlowNodeUUID = nodeId;
    }

    public String getConditionJobFlowNodeUUID() {
        return conditionJobFlowNodeUUID;
    }

    @Override
    protected void validate(JobFlowNodeBuilder jobFlowNodeBuilder){
//        CompositionJobFlowNodeBuilder _compositionJobFlowNodeBuilder =  jobFlowNodeBuilder.getCompositionJobFlowNodeBuilder();
//        if(_compositionJobFlowNodeBuilder != null){
//            if(this.compositionJobFlowNodeBuilder != _compositionJobFlowNodeBuilder)
//                throw new JobFlowBuilderException("已有节点不能重复添加到其他复合节点中");
//        }
    }
    @Override
    public JobFlowNode build(JobFlow jobFlow){
        if(this.jobFlowNode != null){
            return jobFlowNode;
        }
        ConditionJobFlowNode  conditionJobFlowNode = new ConditionJobFlowNode(this.conditionJobFlowNodeUUID);
        conditionJobFlowNode.setAllCondtionNodeMathfailedContinue(this.allCondtionNodeMathfailedContinue);
        conditionJobFlowNode.setNodeId(this.getNodeId());
        conditionJobFlowNode.setNodeName(this.getNodeName());
        conditionJobFlowNode.setJobFlow(jobFlow);
        for(JobFlowNodeBuilder jobFlowNodeBuilder : nodeBuilders) {
            if(jobFlowNodeBuilder == this){
                NodeTrigger conditionNodeTrigger = jobFlowNodeBuilder.getConditionNodeTrigger(this.conditionJobFlowNodeUUID);
                conditionJobFlowNode.addJobFlowNode(conditionJobFlowNode,conditionNodeTrigger);
                if(jobFlowNodeBuilder.isDefaultConditionNode(this.conditionJobFlowNodeUUID)){
                    conditionJobFlowNode.setDefaultJobFlowNode(conditionJobFlowNode);
                }
            }
            else {
                JobFlowNode jobFlowNode = jobFlowNodeBuilder.build(jobFlow);
                NodeTrigger conditionNodeTrigger = jobFlowNodeBuilder.getConditionNodeTrigger(this.conditionJobFlowNodeUUID);
                conditionJobFlowNode.addJobFlowNode(jobFlowNode,conditionNodeTrigger);
                if(jobFlowNodeBuilder.isDefaultConditionNode(this.conditionJobFlowNodeUUID)){
                    conditionJobFlowNode.setDefaultJobFlowNode(jobFlowNode);
                }
            }
            
            
        }
       
       
        this.jobFlowNode = conditionJobFlowNode;
        if (this.parentJobFlowNodeBuilder != null) {
            jobFlowNode.setParentJobFlowNode(parentJobFlowNodeBuilder.getJobFlowNode());
        }
        if(this.nextJobFlowNodeBuilder != null){
            JobFlowNode nextJobFlowNode = nextJobFlowNodeBuilder.build(jobFlow);
            this.jobFlowNode.setNextJobFlowNode(nextJobFlowNode);
        }
        jobFlowNode.setJobFlowNodeListeners(this.jobFlowNodeListeners);
        return jobFlowNode;

       
        
    }

    /**
     * 添加复杂并行子任务，存在串行多个子任务或者串行任务
     * @param jobFlowNodeBuilder
     * @return
     */
    public ConditionJobFlowNodeBuilder addJobFlowNodeBuilder(JobFlowNodeBuilder jobFlowNodeBuilder, NodeTrigger conditionNodeTrigger){
//        this.nodeBuilder = importBuilderCreate.createImportBuilder(this);
        super.addJobFlowNodeBuilder(jobFlowNodeBuilder);
        jobFlowNodeBuilder.setConditionNodeTrigger(this.conditionJobFlowNodeUUID,conditionNodeTrigger);
        return this;
    }

    /**
     * 第一次执行条件节点时，所有节点条件都不匹配时，是否继续执行条件节点后续节点
     * true 执行，false 终止执行，默认值false
     * 需要在添加第一个条件节点时时，进行设置
     */
    public boolean isAllCondtionNodeMathfailedContinue() {
        return allCondtionNodeMathfailedContinue;
    }
    /**
     * 第一次执行条件节点时，所有节点条件都不匹配时，是否继续执行条件节点后续节点
     * true 执行，false 终止执行，默认值false
     * 需要在添加第一个条件节点时时，进行设置
     */
    public void setAllCondtionNodeMathfailedContinue(boolean allCondtionNodeMathfailedContinue) {
        this.allCondtionNodeMathfailedContinue = allCondtionNodeMathfailedContinue;
    }


}
