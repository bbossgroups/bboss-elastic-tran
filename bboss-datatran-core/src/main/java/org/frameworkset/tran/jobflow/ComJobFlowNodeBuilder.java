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
public class ComJobFlowNodeBuilder extends JobFlowNodeBuilder{ 
    
    
    
    
    public ComJobFlowNodeBuilder buildImportBuilder(ImportBuilderCreate importBuilderCreate,NodeTriggerCreate nodeTriggerCreate){
        this.importBuilderCreate = importBuilderCreate;
        this.nodeTriggerCreate = nodeTriggerCreate;
        return this;
    }

    public ComJobFlowNodeBuilder buildImportBuilder(ImportBuilderCreate importBuilderCreate){
        this.importBuilderCreate = importBuilderCreate;
        return this;
    }

    @Override
    public JobFlowNode build(JobFlow jobFlow){
        ComJobFlowNode comJobFlowNode = null;
        if(nodeTriggerCreate != null) {
            comJobFlowNode = new ComJobFlowNode(this.importBuilderCreate.createImportBuilder(this),
                    this.nodeTriggerCreate.createNodeTrigger(this));
        }
        else{
            comJobFlowNode = new ComJobFlowNode(this.importBuilderCreate.createImportBuilder(this));
        }
        comJobFlowNode.setNodeId(this.getNodeId());
        comJobFlowNode.setNodeName(this.getNodeName());
        comJobFlowNode.setJobFlow(jobFlow);
        if(this.parentJobFlowNodeBuilder != null) {
            comJobFlowNode.setParentJobFlowNode(parentJobFlowNodeBuilder.getJobFlowNode());
        }
       
        this.jobFlowNode = comJobFlowNode;
        if(this.nextJobFlowNodeBuilder != null){
            JobFlowNode nextJobFlowNode = nextJobFlowNodeBuilder.build(jobFlow);
            this.jobFlowNode.setNextJobFlowNode(nextJobFlowNode);
        }
        return comJobFlowNode;
        
    }
 
    
    

}
