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

import org.frameworkset.tran.jobflow.ConditionJobFlowNode;
import org.frameworkset.tran.jobflow.JobFlow;
import org.frameworkset.tran.jobflow.JobFlowNode;
import org.frameworkset.tran.jobflow.JobFlowNodeType;


/**
 * 条件作业节点工作流作业节点
 * @author biaoping.yin
 * @Date 2025/3/31
 */
public class ConditionJobFlowNodeBuilder extends CompositionJobFlowNodeBuilder<ConditionJobFlowNodeBuilder> {


    public ConditionJobFlowNodeBuilder(){
        super(JobFlowNodeType.CONDITION);
    }
 
    @Override
    public JobFlowNode build(JobFlow jobFlow){
        if(this.jobFlowNode != null){
            return jobFlowNode;
        }
        ConditionJobFlowNode  conditionJobFlowNode = new ConditionJobFlowNode();
        conditionJobFlowNode.setNodeId(this.getNodeId());
        conditionJobFlowNode.setNodeName(this.getNodeName());
        conditionJobFlowNode.setJobFlow(jobFlow);
        for(JobFlowNodeBuilder jobFlowNodeBuilder : nodeBuilders) {
            JobFlowNode jobFlowNode = jobFlowNodeBuilder.build(jobFlow);
            conditionJobFlowNode.addJobFlowNode(jobFlowNode);
            if(jobFlowNodeBuilder.isDefaultConditionNode()){
                conditionJobFlowNode.setDefaultJobFlowNode(jobFlowNode);
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




}
