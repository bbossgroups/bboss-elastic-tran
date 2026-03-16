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

import org.frameworkset.spi.BBOSSVersion;
import org.frameworkset.tran.TranVersion;
import org.frameworkset.tran.jobflow.JobFlow;
import org.frameworkset.tran.jobflow.NodeTrigger;
import org.frameworkset.tran.jobflow.listener.JobFlowListener;
import org.frameworkset.tran.jobflow.schedule.JobFlowScheduleConfig;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * <p>Description: 作业调度流程编排构建器</p>
 *
 * @author biaoping.yin
 * @Date 2025/3/31
 */
public class JobFlowBuilder {
    static{
        BBOSSVersion.getVersion();
        TranVersion.getVersion();
    }

    private Map<String,ConditionJobFlowNodeBuilder> conditionJobFlowNodeBuilders = new LinkedHashMap<>();
    /**
     * 作业流程id
     */
    private String jobFlowId;
    /**
     * 作业流程名称
     */
    private String jobFlowName;
    private JobFlowScheduleConfig jobFlowScheduleConfig;
    private JobFlowNodeBuilder headerJobFlowNodeBuilder;
    private JobFlowNodeBuilder currentJobFlowNodeBuilder;
    private boolean externalTimer;

    private List<JobFlowListener> jobFlowListeners;
    public JobFlowBuilder setJobFlowId(String jobFlowId){
        this.jobFlowId = jobFlowId;
        return this;
        
    }

    public JobFlowBuilder addJobFlowListener(JobFlowListener jobFlowListener){
        if(jobFlowListeners == null){
            jobFlowListeners = new ArrayList<>();
        }
        jobFlowListeners.add(jobFlowListener);
        return this;
    }
    public JobFlowBuilder setJobFlowName(String jobFlowName){
        this.jobFlowName = jobFlowName;
        return this;
    }

    /**
     * 主干流程管理：添加作业节点
     * @param jobFlowNodeBuilder
     * @return
     */
    public JobFlowBuilder addJobFlowNodeBuilder(JobFlowNodeBuilder jobFlowNodeBuilder){
        validateJobFlowNodeBuilder(jobFlowNodeBuilder);
        if(jobFlowNodeBuilder.getCompositionJobFlowNodeBuilder() != null){
            throw new JobFlowBuilderException("串行分支节点或者并行分支节点中的作业节点不能添加到主干流程中");
        }
        if(jobFlowNodeBuilder.getJobFlowBuilder() != null){
            throw new JobFlowBuilderException("作业节点不能重复添加到主干流程中");
        }
        jobFlowNodeBuilder.setJobFlowBuilder(this);
        if(this.headerJobFlowNodeBuilder == null) {
            this.headerJobFlowNodeBuilder = jobFlowNodeBuilder;

        }
        
        if(currentJobFlowNodeBuilder != null)
            this.currentJobFlowNodeBuilder.setNextJobFlowNodeBuilder(jobFlowNodeBuilder);
        this.currentJobFlowNodeBuilder = jobFlowNodeBuilder;


        return this;
    }
    private void validateJobFlowNodeBuilder(JobFlowNodeBuilder jobFlowNodeBuilder){
        if(jobFlowNodeBuilder.getCompositionJobFlowNodeBuilder() != null){
            throw new JobFlowBuilderException("串行分支节点或者并行分支节点中的作业节点不能添加到主干流程中");
        }

    }
    /**
     * 主干流程管理：为当前作业节点添加后续条件分支，可以连续添加多个
     * @param jobFlowNodeBuilder
     * @return
     */
    public String addConditionJobFlowNodeBuilder(JobFlowNodeBuilder jobFlowNodeBuilder){
        return addConditionJobFlowNodeBuilder(jobFlowNodeBuilder,false);
    }
    




 
    /**
     * 主干流程管理：为当前作业节点添加后续条件分支
     * @param jobFlowNodeBuilder
     * @param defaultConditionNode 是否默认条件节点,条件节点必须配置一个默认流程节点
     * @return
     */
    public String addConditionJobFlowNodeBuilder(JobFlowNodeBuilder jobFlowNodeBuilder,boolean defaultConditionNode){
        return addConditionJobFlowNodeBuilder(  jobFlowNodeBuilder, (NodeTrigger)null,  defaultConditionNode);
    }

    /**
     * 主干流程管理：为当前作业节点添加后续条件分支，可以连续添加多个
     * @param conditionNodeId
     * @return
     */
    public String addConditionJobFlowNodeBuilder(String conditionNodeId){
        return addConditionJobFlowNodeBuilder(conditionNodeId,false);
    }


    /**
     * 主干流程管理：为当前作业节点添加后续条件分支
     * @param conditionNodeId
     * @param defaultConditionNode 是否默认条件节点,条件节点必须配置一个默认流程节点
     * @return
     */
    public String addConditionJobFlowNodeBuilder(String conditionNodeId,boolean defaultConditionNode){
        ConditionJobFlowNodeBuilder jobFlowNodeBuilder = conditionJobFlowNodeBuilders.get(conditionNodeId);
        if(jobFlowNodeBuilder == null){
            throw new JobFlowBuilderException("条件节点"+conditionNodeId+"不存在");
        }
        return addConditionJobFlowNodeBuilder(jobFlowNodeBuilder,  defaultConditionNode);
    }
    /**
     * 主干流程管理：为当前作业节点添加后续条件分支，如果当前节点是一个复合条件节点，则为在该复合条件节点后新加一个条件复合节点，新复合节点后续条件分支就可以直接调用
     * addConditionJobFlowNodeBuilder方法添加
     * 返回条件复合节点唯一ID
     * @param jobFlowNodeBuilder
     * @return 条件复合节点唯一ID
     */
    public String addAnotherConditionJobFlowNodeBuilder(JobFlowNodeBuilder jobFlowNodeBuilder){
        return addAnotherConditionJobFlowNodeBuilder(jobFlowNodeBuilder,false);
    }
    /**
     * 主干流程管理：为当前作业节点添加后续条件分支，如果当前节点是一个复合条件节点，则为在该复合条件节点后新加一个条件复合节点，新复合节点后续条件分支就可以直接调用
     * addConditionJobFlowNodeBuilder方法添加
     * @param jobFlowNodeBuilder
     * @param defaultConditionNode 是否默认条件节点,条件节点必须配置一个默认流程节点
     * @return 条件复合节点唯一ID
     */
    public String addAnotherConditionJobFlowNodeBuilder(JobFlowNodeBuilder jobFlowNodeBuilder,boolean defaultConditionNode){
        return addAnotherConditionJobFlowNodeBuilder(  jobFlowNodeBuilder, (NodeTrigger) null,  defaultConditionNode);
    }

    /**
     * 主干流程管理：为当前作业节点添加后续条件分支，如果当前节点是一个复合条件节点，则为在该复合条件节点后新加一个条件复合节点，新复合节点后续条件分支就可以直接调用
     * addConditionJobFlowNodeBuilder方法添加
     * 返回条件复合节点唯一ID
     * @param conditionNodeId 条件节点ID
     * @return 条件复合节点唯一ID
     */
    public String addAnotherConditionJobFlowNodeBuilder(String conditionNodeId){
        return addAnotherConditionJobFlowNodeBuilder(conditionNodeId,false);
    }
    /**
     * 主干流程管理：为当前作业节点添加后续条件分支，如果当前节点是一个复合条件节点，则为在该复合条件节点后新加一个条件复合节点，新复合节点后续条件分支就可以直接调用
     * addConditionJobFlowNodeBuilder方法添加
     * @param conditionNodeId 条件节点ID
     * @param defaultConditionNode 是否默认条件节点,条件节点必须配置一个默认流程节点
     * @return 条件复合节点唯一ID
     */
    public String addAnotherConditionJobFlowNodeBuilder(String conditionNodeId,boolean defaultConditionNode){
        ConditionJobFlowNodeBuilder jobFlowNodeBuilder = conditionJobFlowNodeBuilders.get(conditionNodeId);
        if(jobFlowNodeBuilder == null){
            throw new JobFlowBuilderException("条件节点"+conditionNodeId+"不存在");
        }
        return addAnotherConditionJobFlowNodeBuilder(jobFlowNodeBuilder,  defaultConditionNode);
    }


    /**
     * 主干流程管理：为当前作业节点添加后续条件分支，可以连续添加多个
     * @param jobFlowNodeBuilder
     * @return
     */
    public String addConditionJobFlowNodeBuilder(JobFlowNodeBuilder jobFlowNodeBuilder, NodeTrigger conditionNodeTrigger){
        return addConditionJobFlowNodeBuilder(jobFlowNodeBuilder,   conditionNodeTrigger,false);
    }






    /**
     * 主干流程管理：为当前作业节点添加后续条件分支
     * @param jobFlowNodeBuilder
     * @param defaultConditionNode 是否默认条件节点,条件节点必须配置一个默认流程节点
     * @return
     */
    public String addConditionJobFlowNodeBuilder(JobFlowNodeBuilder jobFlowNodeBuilder, NodeTrigger conditionNodeTrigger,boolean defaultConditionNode){
        validateJobFlowNodeBuilder(jobFlowNodeBuilder);

        String cid = null;
        if(currentJobFlowNodeBuilder != null) {
            if(currentJobFlowNodeBuilder instanceof ConditionJobFlowNodeBuilder){
                ((ConditionJobFlowNodeBuilder)currentJobFlowNodeBuilder).addJobFlowNodeBuilder(jobFlowNodeBuilder,   conditionNodeTrigger);
                cid = ((ConditionJobFlowNodeBuilder)currentJobFlowNodeBuilder).getConditionJobFlowNodeUUID();
            }
            else {


                ConditionJobFlowNodeBuilder conditionJobFlowNodeBuilder = new ConditionJobFlowNodeBuilder();
                conditionJobFlowNodeBuilder.addJobFlowNodeBuilder(jobFlowNodeBuilder,   conditionNodeTrigger);
                currentJobFlowNodeBuilder.setNextJobFlowNodeBuilder(conditionJobFlowNodeBuilder);
                currentJobFlowNodeBuilder = conditionJobFlowNodeBuilder;
                cid = conditionJobFlowNodeBuilder.getConditionJobFlowNodeUUID();
                conditionJobFlowNodeBuilders.put(cid,conditionJobFlowNodeBuilder);
            }

        }
        else{
            ConditionJobFlowNodeBuilder conditionJobFlowNodeBuilder = new ConditionJobFlowNodeBuilder();
            conditionJobFlowNodeBuilder.addJobFlowNodeBuilder(jobFlowNodeBuilder);
            this.currentJobFlowNodeBuilder = conditionJobFlowNodeBuilder;
            if(this.headerJobFlowNodeBuilder == null) {
                this.headerJobFlowNodeBuilder = conditionJobFlowNodeBuilder;

            }
            cid = conditionJobFlowNodeBuilder.getConditionJobFlowNodeUUID();
            conditionJobFlowNodeBuilders.put(cid,conditionJobFlowNodeBuilder);
        }
        jobFlowNodeBuilder.setDefaultConditionNode(cid,defaultConditionNode);
        return cid;
    }

    /**
     * 主干流程管理：为当前作业节点添加后续条件分支，可以连续添加多个
     * @param conditionNodeId
     * @return
     */
    public String addConditionJobFlowNodeBuilder(String conditionNodeId, NodeTrigger conditionNodeTrigger){
        return addConditionJobFlowNodeBuilder(conditionNodeId ,   conditionNodeTrigger,false);
    }


    /**
     * 主干流程管理：为当前作业节点添加后续条件分支
     * @param conditionNodeId
     * @param defaultConditionNode 是否默认条件节点,条件节点必须配置一个默认流程节点
     * @return
     */
    public String addConditionJobFlowNodeBuilder(String conditionNodeId, NodeTrigger conditionNodeTrigger,boolean defaultConditionNode){
        ConditionJobFlowNodeBuilder jobFlowNodeBuilder = conditionJobFlowNodeBuilders.get(conditionNodeId);
        if(jobFlowNodeBuilder == null){
            throw new JobFlowBuilderException("条件节点"+conditionNodeId+"不存在");
        }
        return addConditionJobFlowNodeBuilder(jobFlowNodeBuilder,   conditionNodeTrigger,  defaultConditionNode);
    }
    /**
     * 主干流程管理：为当前作业节点添加后续条件分支，如果当前节点是一个复合条件节点，则为在该复合条件节点后新加一个条件复合节点，新复合节点后续条件分支就可以直接调用
     * addConditionJobFlowNodeBuilder方法添加
     * 返回条件复合节点唯一ID
     * @param jobFlowNodeBuilder
     * @return 条件复合节点唯一ID
     */
    public String addAnotherConditionJobFlowNodeBuilder(JobFlowNodeBuilder jobFlowNodeBuilder, NodeTrigger conditionNodeTrigger){
        return addAnotherConditionJobFlowNodeBuilder(jobFlowNodeBuilder,   conditionNodeTrigger,false);
    }
    /**
     * 主干流程管理：为当前作业节点添加后续条件分支，如果当前节点是一个复合条件节点，则为在该复合条件节点后新加一个条件复合节点，新复合节点后续条件分支就可以直接调用
     * addConditionJobFlowNodeBuilder方法添加
     * @param jobFlowNodeBuilder
     * @param defaultConditionNode 是否默认条件节点,条件节点必须配置一个默认流程节点
     * @return 条件复合节点唯一ID
     */
    public String addAnotherConditionJobFlowNodeBuilder(JobFlowNodeBuilder jobFlowNodeBuilder, NodeTrigger conditionNodeTrigger,boolean defaultConditionNode){
        validateJobFlowNodeBuilder(jobFlowNodeBuilder);
        String cid = null;
        if(currentJobFlowNodeBuilder != null) {
            ConditionJobFlowNodeBuilder conditionJobFlowNodeBuilder = new ConditionJobFlowNodeBuilder();
            conditionJobFlowNodeBuilder.addJobFlowNodeBuilder(jobFlowNodeBuilder,   conditionNodeTrigger);
            currentJobFlowNodeBuilder.setNextJobFlowNodeBuilder(conditionJobFlowNodeBuilder);
            currentJobFlowNodeBuilder = conditionJobFlowNodeBuilder;
            cid = conditionJobFlowNodeBuilder.getConditionJobFlowNodeUUID();
            conditionJobFlowNodeBuilders.put(cid,conditionJobFlowNodeBuilder);

        }
        else{
            ConditionJobFlowNodeBuilder conditionJobFlowNodeBuilder = new ConditionJobFlowNodeBuilder();
            conditionJobFlowNodeBuilder.addJobFlowNodeBuilder(jobFlowNodeBuilder,   conditionNodeTrigger);
            this.currentJobFlowNodeBuilder = conditionJobFlowNodeBuilder;
            if(this.headerJobFlowNodeBuilder == null) {
                this.headerJobFlowNodeBuilder = conditionJobFlowNodeBuilder;

            }
            cid = conditionJobFlowNodeBuilder.getConditionJobFlowNodeUUID();
            conditionJobFlowNodeBuilders.put(cid,conditionJobFlowNodeBuilder);
        }

        jobFlowNodeBuilder.setDefaultConditionNode(cid,defaultConditionNode);
        return cid;
    }

    /**
     * 主干流程管理：为当前作业节点添加后续条件分支，如果当前节点是一个复合条件节点，则为在该复合条件节点后新加一个条件复合节点，新复合节点后续条件分支就可以直接调用
     * addConditionJobFlowNodeBuilder方法添加
     * 返回条件复合节点唯一ID
     * @param conditionNodeId 条件节点ID
     * @return 条件复合节点唯一ID
     */
    public String addAnotherConditionJobFlowNodeBuilder(String conditionNodeId, NodeTrigger conditionNodeTrigger){
        return addAnotherConditionJobFlowNodeBuilder(conditionNodeId,   conditionNodeTrigger,false);
    }
    /**
     * 主干流程管理：为当前作业节点添加后续条件分支，如果当前节点是一个复合条件节点，则为在该复合条件节点后新加一个条件复合节点，新复合节点后续条件分支就可以直接调用
     * addConditionJobFlowNodeBuilder方法添加
     * @param conditionNodeId 条件节点ID
     * @param defaultConditionNode 是否默认条件节点,条件节点必须配置一个默认流程节点
     * @return 条件复合节点唯一ID
     */
    public String addAnotherConditionJobFlowNodeBuilder(String conditionNodeId, NodeTrigger conditionNodeTrigger, boolean defaultConditionNode){
        ConditionJobFlowNodeBuilder jobFlowNodeBuilder = conditionJobFlowNodeBuilders.get(conditionNodeId);
        if(jobFlowNodeBuilder == null){
            throw new JobFlowBuilderException("条件节点"+conditionNodeId+"不存在");
        }
        return addAnotherConditionJobFlowNodeBuilder(jobFlowNodeBuilder,   conditionNodeTrigger,  defaultConditionNode);
    }



    /**
     * 添加作业节点
     * 建议使用方法：addJobFlowNodeBuilder(JobFlowNodeBuilder jobFlowNodeBuilder)
     * @param jobFlowNodeBuilder
     * @return
     */
    @Deprecated
    public JobFlowBuilder addJobFlowNode(JobFlowNodeBuilder jobFlowNodeBuilder){      
        return addJobFlowNodeBuilder(  jobFlowNodeBuilder);
    }
    
    public JobFlow build(){
        JobFlow jobFlow = new JobFlow();
        jobFlow.setJobFlowName(this.jobFlowName);
        jobFlow.setJobFlowId(this.jobFlowId);
        jobFlow.setJobFlowListeners(this.jobFlowListeners);
        if(!isExternalTimer()) {
            jobFlow.setJobScheduleConfig(jobFlowScheduleConfig);
        }
        else{
            jobFlow.setExternalTimer(this.isExternalTimer());
        }
        jobFlow.initJobInfo();
        jobFlow.initGroovyClassLoader();
        jobFlow.setStartJobFlowNode(headerJobFlowNodeBuilder.build(jobFlow));    
        
        return jobFlow;
    }

    public boolean isExternalTimer() {
        return externalTimer;
    }

    public JobFlowBuilder setExternalTimer(boolean externalTimer) {
        this.externalTimer = externalTimer;
        return this;
    }

    public void setJobFlowScheduleConfig(JobFlowScheduleConfig jobFlowScheduleConfig) {
        this.jobFlowScheduleConfig = jobFlowScheduleConfig;
    }
}
