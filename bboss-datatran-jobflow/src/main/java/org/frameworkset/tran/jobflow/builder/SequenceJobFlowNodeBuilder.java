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

import org.frameworkset.tran.jobflow.*;
import org.frameworkset.tran.jobflow.script.TriggerScriptAPI;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 
 * @author biaoping.yin
 * @Date 2025/3/31
 */
public class SequenceJobFlowNodeBuilder extends CompositionJobFlowNodeBuilder<SequenceJobFlowNodeBuilder> {

    private JobFlowNodeBuilder headerJobFlowNodeBuilder;
    private JobFlowNodeBuilder currentJobFlowNodeBuilder;
    private Map<String,ConditionJobFlowNodeBuilder> conditionJobFlowNodeBuilders = new LinkedHashMap<>();
    private Map<String ,JobFlowNodeBuilder> jobFlowNodeBuilderMap = new LinkedHashMap<>();
   
    public SequenceJobFlowNodeBuilder(String nodeId,String nodeName){
        super(nodeId,nodeName,JobFlowNodeType.SEQUENCE);
    }
    public SequenceJobFlowNodeBuilder( String nodeName){
        super(null,nodeName,JobFlowNodeType.SEQUENCE);
    }

    public SequenceJobFlowNodeBuilder(){
        super(JobFlowNodeType.SEQUENCE);
    }

    public JobFlowNodeBuilder getJobFlowNodeBuilder(String nodeId){
        return jobFlowNodeBuilderMap.get(nodeId);
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
        if(!jobFlowNodeBuilderMap.containsKey(jobFlowNodeBuilder.getNodeId())) {
            jobFlowNodeBuilderMap.put(jobFlowNodeBuilder.getNodeId(),jobFlowNodeBuilder);
        }
        
        return this;
    }
    
    /**
     * 串行流程节点管理：为当前作业节点添加带条件的下一个作业节点
     * @param jobFlowNodeBuilder
     * @return
     */
    public String addConditionJobFlowNodeBuilder(JobFlowNodeBuilder jobFlowNodeBuilder){
        return addConditionJobFlowNodeBuilder(jobFlowNodeBuilder,false);
    }

    /**
     * 串行流程节点管理：为当前作业节点添加带条件的下一个作业节点
     * @param jobFlowNodeBuilder
     * @param nodeTrigger 指定条件节点条件触发器
     * @return
     */
    public String addConditionJobFlowNodeBuilder(JobFlowNodeBuilder jobFlowNodeBuilder, NodeTrigger nodeTrigger){
        return addConditionJobFlowNodeBuilder(jobFlowNodeBuilder,nodeTrigger,false);
    }

    /**
     * 串行流程节点管理：为当前作业节点添加带条件的下一个作业节点
     * @param jobFlowNodeBuilder
     * @param defaultConditionNode 是否默认条件节点,条件节点必须配置一个默认流程节点
     * @return
     */
    public String addConditionJobFlowNodeBuilder(JobFlowNodeBuilder jobFlowNodeBuilder,boolean defaultConditionNode){
        return addConditionJobFlowNodeBuilder(jobFlowNodeBuilder, (NodeTrigger) null,defaultConditionNode);
    }
    /**
     * 串行流程节点管理：为当前作业节点添加带条件的下一个作业节点
     * @param jobFlowNodeBuilder
     * @param conditionNodeTrigger 节点条件触发器
     * @param defaultConditionNode 是否默认条件节点,条件节点必须配置一个默认流程节点
     * @return
     */
    public String addConditionJobFlowNodeBuilder(JobFlowNodeBuilder jobFlowNodeBuilder, NodeTrigger conditionNodeTrigger,boolean defaultConditionNode){
         
        return addConditionJobFlowNodeBuilder(  false,  jobFlowNodeBuilder,   conditionNodeTrigger,  defaultConditionNode);
    }
    /**
     * 串行流程节点管理：为当前作业节点添加带条件的下一个作业节点
     * @param jobFlowNodeBuilder
     * @param conditionNodeTrigger 节点条件触发器
     * @param defaultConditionNode 是否默认条件节点,条件节点必须配置一个默认流程节点
     * @return
     */
    public String addConditionJobFlowNodeBuilder(boolean allCondtionNodeMathfailedContinue,JobFlowNodeBuilder jobFlowNodeBuilder, NodeTrigger conditionNodeTrigger,boolean defaultConditionNode){
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
        String cid = null;
        if(currentJobFlowNodeBuilder != null) {
            if(currentJobFlowNodeBuilder instanceof ConditionJobFlowNodeBuilder){
                ((ConditionJobFlowNodeBuilder)currentJobFlowNodeBuilder).addJobFlowNodeBuilder(jobFlowNodeBuilder,   conditionNodeTrigger);
                cid = ((ConditionJobFlowNodeBuilder)currentJobFlowNodeBuilder).getConditionJobFlowNodeUUID();
            }
            else {
                ConditionJobFlowNodeBuilder conditionJobFlowNodeBuilder = new ConditionJobFlowNodeBuilder();
                conditionJobFlowNodeBuilder.addJobFlowNodeBuilder(jobFlowNodeBuilder);
                conditionJobFlowNodeBuilder.setAllCondtionNodeMatchfailedContinue(allCondtionNodeMathfailedContinue);
                currentJobFlowNodeBuilder.setNextJobFlowNodeBuilder(conditionJobFlowNodeBuilder);
                nodeBuilders.add(conditionJobFlowNodeBuilder);
                currentJobFlowNodeBuilder = conditionJobFlowNodeBuilder;
                cid = conditionJobFlowNodeBuilder.getConditionJobFlowNodeUUID();
                conditionJobFlowNodeBuilders.put(cid, conditionJobFlowNodeBuilder);
                this.jobFlowNodeBuilderMap.put(cid, conditionJobFlowNodeBuilder);
            }

        }
        else{
            ConditionJobFlowNodeBuilder conditionJobFlowNodeBuilder = new ConditionJobFlowNodeBuilder();
            conditionJobFlowNodeBuilder.setAllCondtionNodeMatchfailedContinue(allCondtionNodeMathfailedContinue);
            conditionJobFlowNodeBuilder.addJobFlowNodeBuilder(jobFlowNodeBuilder);
            this.currentJobFlowNodeBuilder = conditionJobFlowNodeBuilder;
            nodeBuilders.add(conditionJobFlowNodeBuilder);
            if(this.headerJobFlowNodeBuilder == null) {
                this.headerJobFlowNodeBuilder = conditionJobFlowNodeBuilder;

            }
            cid = conditionJobFlowNodeBuilder.getConditionJobFlowNodeUUID();
            conditionJobFlowNodeBuilders.put(cid, conditionJobFlowNodeBuilder);

            this.jobFlowNodeBuilderMap.put(cid, conditionJobFlowNodeBuilder);
        }
        if(!jobFlowNodeBuilderMap.containsKey(jobFlowNodeBuilder.getNodeId())) {
            this.jobFlowNodeBuilderMap.put(jobFlowNodeBuilder.getNodeId(), jobFlowNodeBuilder);
        }
        
        jobFlowNodeBuilder.setDefaultConditionNode(cid,defaultConditionNode);
        return cid;
    }

    

    /**
     * 串行流程节点管理：为当前作业节点添加带条件的下一个作业节点
     * @param conditionNodeId
     * @return
     */
    public String addConditionJobFlowNodeBuilder(String  conditionNodeId){
        return addConditionJobFlowNodeBuilder(conditionNodeId,false);
    }
    /**
     * 串行流程节点管理：为当前作业节点添加带条件的下一个作业节点
     * @param conditionNodeId
     * @param defaultConditionNode 是否默认条件节点,条件节点必须配置一个默认流程节点
     * @return
     */
    public String addConditionJobFlowNodeBuilder(String  conditionNodeId,boolean defaultConditionNode){
        JobFlowNodeBuilder jobFlowNodeBuilder = conditionJobFlowNodeBuilders.get(conditionNodeId);
        if(jobFlowNodeBuilder == null){
            throw new JobFlowBuilderException("条件节点"+conditionNodeId+"不存在");
        }
        return this.addConditionJobFlowNodeBuilder(jobFlowNodeBuilder,defaultConditionNode);
         
    }

    /**
     * 串行流程节点管理：为当前作业节点添加带条件的下一个作业节点
     * @param conditionNodeId
     * @return
     */
    public String addConditionJobFlowNodeBuilder(String  conditionNodeId, NodeTrigger nodeTrigger){
        return addConditionJobFlowNodeBuilder(conditionNodeId,nodeTrigger,false);
    }
    /**
     * 串行流程节点管理：为当前作业节点添加带条件的下一个作业节点
     * @param conditionNodeId
     * @param nodeTrigger 指定条件节点条件触发器 
     * @param defaultConditionNode 是否默认条件节点,条件节点必须配置一个默认流程节点
     * @return
     */
    public String addConditionJobFlowNodeBuilder(String  conditionNodeId, NodeTrigger nodeTrigger,boolean defaultConditionNode){
        JobFlowNodeBuilder jobFlowNodeBuilder = conditionJobFlowNodeBuilders.get(conditionNodeId);
        if(jobFlowNodeBuilder == null){
            throw new JobFlowBuilderException("条件节点"+conditionNodeId+"不存在");
        }
        return this.addConditionJobFlowNodeBuilder(jobFlowNodeBuilder,nodeTrigger,defaultConditionNode);

    }
    
    
    //************************************//

    /**
     * 主干流程管理：为当前作业节点添加后续条件分支，可以连续添加多个
     * @param conditionNodeId
     * @return
     */
    public String addConditionJobFlowNodeBuilder(String conditionNodeId, TriggerScriptAPI conditionNodeTrigger){
        return addConditionJobFlowNodeBuilder(conditionNodeId ,   conditionNodeTrigger,false);
    }

    /**
     * 主干流程管理：为当前作业节点添加后续条件分支
     * @param conditionNodeId
     * @param defaultConditionNode 是否默认条件节点,条件节点必须配置一个默认流程节点
     * @return
     */
    public String addConditionJobFlowNodeBuilder(String conditionNodeId, TriggerScriptAPI conditionNodeTrigger,boolean defaultConditionNode){
        ConditionJobFlowNodeBuilder jobFlowNodeBuilder = conditionJobFlowNodeBuilders.get(conditionNodeId);
        if(jobFlowNodeBuilder == null){
            throw new JobFlowBuilderException("条件节点"+conditionNodeId+"不存在");
        }
        return addConditionJobFlowNodeBuilder(jobFlowNodeBuilder,   conditionNodeTrigger != null?new NodeTrigger(conditionNodeTrigger):null,  defaultConditionNode);
    }
    /**
     * 主干流程管理：为当前作业节点添加后续条件分支
     * @param conditionNodeId
     * @return
     */
    public String addConditionJobFlowNodeBuilder(boolean allCondtionNodeMathfailedContinue,String conditionNodeId, TriggerScriptAPI conditionNodeTrigger){

        return addConditionJobFlowNodeBuilder(  allCondtionNodeMathfailedContinue,  conditionNodeId,   conditionNodeTrigger,false);
    }
    /**
     * 主干流程管理：为当前作业节点添加后续条件分支
     * @param conditionNodeId
     * @param defaultConditionNode 是否默认条件节点,条件节点必须配置一个默认流程节点
     * @return
     */
    public String addConditionJobFlowNodeBuilder(boolean allCondtionNodeMathfailedContinue,String conditionNodeId, TriggerScriptAPI conditionNodeTrigger,boolean defaultConditionNode){
        ConditionJobFlowNodeBuilder jobFlowNodeBuilder = conditionJobFlowNodeBuilders.get(conditionNodeId);
        if(jobFlowNodeBuilder == null){
            throw new JobFlowBuilderException("条件节点"+conditionNodeId+"不存在");
        }
        return addConditionJobFlowNodeBuilder(  allCondtionNodeMathfailedContinue,jobFlowNodeBuilder,   conditionNodeTrigger != null?new NodeTrigger(conditionNodeTrigger):null,  defaultConditionNode);
    }

    /**
     * 主干流程管理：为当前作业节点添加后续条件分支
     * @param conditionNodeId
     * @param defaultConditionNode 是否默认条件节点,条件节点必须配置一个默认流程节点
     * @return
     */
    public String addConditionJobFlowNodeBuilder(boolean allCondtionNodeMathfailedContinue,String conditionNodeId, NodeTrigger conditionNodeTrigger,boolean defaultConditionNode){
        ConditionJobFlowNodeBuilder jobFlowNodeBuilder = conditionJobFlowNodeBuilders.get(conditionNodeId);
        if(jobFlowNodeBuilder == null){
            throw new JobFlowBuilderException("条件节点"+conditionNodeId+"不存在");
        }
        return addConditionJobFlowNodeBuilder(  allCondtionNodeMathfailedContinue,jobFlowNodeBuilder,   conditionNodeTrigger,  defaultConditionNode);
    }
    
    //***********************************//
    
    

    /**
     * 串行流程节点管理：为当前作业节点添加带条件的下一个作业节点
     * @param jobFlowNodeBuilder
     * @return
     */
    public String addAnotherConditionJobFlowNodeBuilder(JobFlowNodeBuilder jobFlowNodeBuilder){
        return addAnotherConditionJobFlowNodeBuilder(jobFlowNodeBuilder,false);
    }
    /**
     * 串行流程节点管理：为当前作业节点添加带条件的下一个作业节点
     * @param jobFlowNodeBuilder
     * @param defaultConditionNode 是否默认条件节点,条件节点必须配置一个默认流程节点
     * @return
     */
    public String addAnotherConditionJobFlowNodeBuilder(JobFlowNodeBuilder jobFlowNodeBuilder,boolean defaultConditionNode){
        return addAnotherConditionJobFlowNodeBuilder( jobFlowNodeBuilder,(NodeTrigger)null, defaultConditionNode);
    }

    /**
     * 串行流程节点管理：为当前作业节点添加带条件的下一个作业节点
     * @param conditionNodeId
     * @return
     */
    public String addAnotherConditionJobFlowNodeBuilder(String  conditionNodeId){
        return addAnotherConditionJobFlowNodeBuilder(conditionNodeId,false);
    }
    /**
     * 串行流程节点管理：为当前作业节点添加带条件的下一个作业节点
     * @param conditionNodeId
     * @param defaultConditionNode 是否默认条件节点,条件节点必须配置一个默认流程节点
     * @return
     */
    public String addAnotherConditionJobFlowNodeBuilder(String  conditionNodeId,boolean defaultConditionNode){
        
        return this.addAnotherConditionJobFlowNodeBuilder(conditionNodeId,(NodeTrigger) null,defaultConditionNode);
    }


    /**
     * 串行流程节点管理：为当前作业节点添加带条件的下一个作业节点
     * @param jobFlowNodeBuilder
     * @param nodeTrigger 指定条件节点条件触发器 
     * @return
     */
    public String addAnotherConditionJobFlowNodeBuilder(JobFlowNodeBuilder jobFlowNodeBuilder,NodeTrigger nodeTrigger){
        return addAnotherConditionJobFlowNodeBuilder(jobFlowNodeBuilder,  nodeTrigger,false);
    }
    public String addAnotherConditionJobFlowNodeBuilder( JobFlowNodeBuilder jobFlowNodeBuilder, NodeTrigger conditionNodeTrigger, boolean defaultConditionNode) {
        return addAnotherConditionJobFlowNodeBuilder(false, jobFlowNodeBuilder, conditionNodeTrigger, defaultConditionNode);
    }
    /**
     * 串行流程节点管理：为当前作业节点添加带条件的下一个作业节点
     * @param jobFlowNodeBuilder
     * @param nodeTrigger 指定条件节点条件触发器
     * @param defaultConditionNode 是否默认条件节点,条件节点必须配置一个默认流程节点
     * @return
     */
    public String addAnotherConditionJobFlowNodeBuilder(boolean allCondtionNodeMathfailedContinue,JobFlowNodeBuilder jobFlowNodeBuilder,NodeTrigger nodeTrigger,boolean defaultConditionNode){
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
        if(!jobFlowNodeBuilderMap.containsKey(jobFlowNodeBuilder.getNodeId())) {
            this.jobFlowNodeBuilderMap.put(jobFlowNodeBuilder.getNodeId(), jobFlowNodeBuilder);
        }
        String cid = null;
       
        if(currentJobFlowNodeBuilder != null) {
            ConditionJobFlowNodeBuilder conditionJobFlowNodeBuilder = new ConditionJobFlowNodeBuilder();
            conditionJobFlowNodeBuilder.setAllCondtionNodeMatchfailedContinue(allCondtionNodeMathfailedContinue);
            conditionJobFlowNodeBuilder.addJobFlowNodeBuilder(jobFlowNodeBuilder,  nodeTrigger);
            currentJobFlowNodeBuilder.setNextJobFlowNodeBuilder(conditionJobFlowNodeBuilder);
            nodeBuilders.add(conditionJobFlowNodeBuilder);
            currentJobFlowNodeBuilder = conditionJobFlowNodeBuilder;
            cid = conditionJobFlowNodeBuilder.getConditionJobFlowNodeUUID();

            conditionJobFlowNodeBuilders.put(cid, conditionJobFlowNodeBuilder);
            jobFlowNodeBuilderMap.put(cid, conditionJobFlowNodeBuilder);
        }
        else{
            ConditionJobFlowNodeBuilder conditionJobFlowNodeBuilder = new ConditionJobFlowNodeBuilder();
            conditionJobFlowNodeBuilder.setAllCondtionNodeMatchfailedContinue(allCondtionNodeMathfailedContinue);
            conditionJobFlowNodeBuilder.addJobFlowNodeBuilder(jobFlowNodeBuilder,  nodeTrigger);
            this.currentJobFlowNodeBuilder = conditionJobFlowNodeBuilder;
            nodeBuilders.add(conditionJobFlowNodeBuilder);
            if(this.headerJobFlowNodeBuilder == null) {
                this.headerJobFlowNodeBuilder = conditionJobFlowNodeBuilder;

            }
            cid = conditionJobFlowNodeBuilder.getConditionJobFlowNodeUUID();
            conditionJobFlowNodeBuilders.put(cid, conditionJobFlowNodeBuilder);
            jobFlowNodeBuilderMap.put(cid, conditionJobFlowNodeBuilder);
        }
        
        jobFlowNodeBuilder.setDefaultConditionNode(cid,defaultConditionNode);
        return cid;
    }

    /**
     * 串行流程节点管理：为当前作业节点添加带条件的下一个作业节点
     * @param conditionNodeId
     * @param nodeTrigger 指定条件节点条件触发器 
     * @return
     */
    public String addAnotherConditionJobFlowNodeBuilder(String  conditionNodeId,NodeTrigger nodeTrigger){
        return addAnotherConditionJobFlowNodeBuilder(conditionNodeId,  nodeTrigger,false);
    }
    /**
     * 串行流程节点管理：为当前作业节点添加带条件的下一个作业节点
     * @param conditionNodeId
     * @param nodeTrigger 指定条件节点条件触发器 
     * @param defaultConditionNode 是否默认条件节点,条件节点必须配置一个默认流程节点
     * @return
     */
    public String addAnotherConditionJobFlowNodeBuilder(String  conditionNodeId,NodeTrigger nodeTrigger,boolean defaultConditionNode){
        JobFlowNodeBuilder jobFlowNodeBuilder = conditionJobFlowNodeBuilders.get(conditionNodeId);
        if(jobFlowNodeBuilder == null){
            throw new JobFlowBuilderException("条件节点"+conditionNodeId+"不存在");
        }
        return this.addAnotherConditionJobFlowNodeBuilder(jobFlowNodeBuilder,  nodeTrigger,defaultConditionNode);
    }

    public String addConditionJobFlowNodeBuilder(JobFlowNodeBuilder jobFlowNodeBuilder, TriggerScriptAPI conditionNodeTrigger){
        return addConditionJobFlowNodeBuilder(jobFlowNodeBuilder,   conditionNodeTrigger,false);
    }
    public String addConditionJobFlowNodeBuilder(JobFlowNodeBuilder jobFlowNodeBuilder, TriggerScriptAPI conditionNodeTrigger,boolean defaultConditionNode){
        return addConditionJobFlowNodeBuilder(false,jobFlowNodeBuilder, conditionNodeTrigger,defaultConditionNode);
    }
    public String addConditionJobFlowNodeBuilder(boolean allCondtionNodeMathfailedContinue,JobFlowNodeBuilder jobFlowNodeBuilder, TriggerScriptAPI conditionNodeTrigger,boolean defaultConditionNode){
        return   addConditionJobFlowNodeBuilder(  allCondtionNodeMathfailedContinue,  jobFlowNodeBuilder, new NodeTrigger(conditionNodeTrigger), defaultConditionNode);
    }
    protected SequenceJobFlowNode buildSequenceJobFlowNode(){
        return new SequenceJobFlowNode();
    }
    @Override
    public JobFlowNode build(JobFlow jobFlow){
        if(this.jobFlowNode != null){
            return jobFlowNode;
        }
        SequenceJobFlowNode sequenceJobFlowNode = buildSequenceJobFlowNode();
         
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
