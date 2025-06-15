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

/**
 * <p>Description: </p>
 * <p></p>
 *
 * @author biaoping.yin
 * @Date 2025/3/31
 */
public class SimpleJobFlowNodeBuilder extends JobFlowNodeBuilder{ 
    
    
    
    
    public SimpleJobFlowNodeBuilder buildImportBuilder(ImportBuilderCreate importBuilderCreate, NodeTriggerCreate nodeTriggerCreate){
        this.importBuilderCreate = importBuilderCreate;
        this.nodeTriggerCreate = nodeTriggerCreate;
        return this;
    }

    public SimpleJobFlowNodeBuilder buildImportBuilder(ImportBuilderCreate importBuilderCreate){
        this.importBuilderCreate = importBuilderCreate;
        return this;
    }

    @Override
    public JobFlowNode build(JobFlow jobFlow){
        SimpleJobFlowNode simpleJobFlowNode = null;
        if(importBuilder == null){
            if(importBuilderCreate != null)
                importBuilder = importBuilderCreate.createImportBuilder(this);
        }
        if(nodeTrigger == null) {
            if (nodeTriggerCreate != null) {
                nodeTrigger = this.nodeTriggerCreate.createNodeTrigger(this);
                
            }
        }
        simpleJobFlowNode = new SimpleJobFlowNode(importBuilder,nodeTrigger); 
        simpleJobFlowNode.setNodeId(this.getNodeId());
        simpleJobFlowNode.setNodeName(this.getNodeName());
        simpleJobFlowNode.setJobFlow(jobFlow);
        if(this.parentJobFlowNodeBuilder != null) {
            simpleJobFlowNode.setParentJobFlowNode(parentJobFlowNodeBuilder.getJobFlowNode());
        }
       
        this.jobFlowNode = simpleJobFlowNode;
        if(this.nextJobFlowNodeBuilder != null){
            JobFlowNode nextJobFlowNode = nextJobFlowNodeBuilder.build(jobFlow);
            this.jobFlowNode.setNextJobFlowNode(nextJobFlowNode);
        }
        return simpleJobFlowNode;
        
    }
 
    
    

}
