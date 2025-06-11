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

import org.frameworkset.tran.jobflow.schedule.JobFlowScheduleConfig;

/**
 * <p>Description: 作业调度流程编排构建器</p>
 * <p></p>
 *
 * @author biaoping.yin
 * @Date 2025/3/31
 */
public class JobFlowBuilder {
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
    public JobFlowBuilder setJobFlowId(String jobFlowId){
        this.jobFlowId = jobFlowId;
        return this;
        
    }

    public JobFlowBuilder setJobFlowName(String jobFlowName){
        this.jobFlowName = jobFlowName;
        return this;
    }
    
    public JobFlowBuilder addJobFlowNode(JobFlowNodeBuilder jobFlowNodeBuilder){
        if(this.headerJobFlowNodeBuilder == null) {
            this.headerJobFlowNodeBuilder = jobFlowNodeBuilder;
           
        }
        this.currentJobFlowNodeBuilder.setNextJobFlowNodeBuilder(jobFlowNodeBuilder);
        this.currentJobFlowNodeBuilder = jobFlowNodeBuilder;
            
        
        return this;
    }
    
    public JobFlow build(){
        JobFlow jobFlow = new JobFlow();
        jobFlow.setJobFlowName(this.jobFlowName);
        jobFlow.setJobFlowId(this.jobFlowId);
        jobFlow.setStartJobFlowNode(headerJobFlowNodeBuilder.build(jobFlow));
        jobFlow.setJobScheduleConfig(jobFlowScheduleConfig);
        return jobFlow;
    }

    public boolean isExternalTimer() {
        return externalTimer;
    }

    public void setExternalTimer(boolean externalTimer) {
        this.externalTimer = externalTimer;
    }
}
