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
import org.frameworkset.tran.jobflow.builder.CompositionJobFlowNodeBuilder;

/**
 * 
 * @author biaoping.yin
 * @Date 2025/3/31
 */
public class ParrelJobFlowNodeBuilder extends CompositionJobFlowNodeBuilder {
    
    
    
    public ParrelJobFlowNodeBuilder( ){
        super(JobFlowNodeType.PARREL);
    }


    @Override
    public JobFlowNode build(JobFlow jobFlow){
        ParrelJobFlowNode parrelJobFlowNode = new ParrelJobFlowNode();
        parrelJobFlowNode.setNodeId(this.getNodeId());
        parrelJobFlowNode.setNodeName(this.getNodeName());
        parrelJobFlowNode.setJobFlow(jobFlow);
        if(this.parentJobFlowNodeBuilder != null) {
            parrelJobFlowNode.setParentJobFlowNode(parentJobFlowNodeBuilder.getJobFlowNode());
        }
        if(this.nodeTriggerCreate != null){
            parrelJobFlowNode.setNodeTrigger(this.nodeTriggerCreate.createNodeTrigger(this));
        }
        for(JobFlowNodeBuilder jobFlowNodeBuilder:nodeBuilders){
            parrelJobFlowNode.addJobFlowNode(jobFlowNodeBuilder.build(jobFlow));
        }
        this.jobFlowNode = parrelJobFlowNode;
        if(this.nextJobFlowNodeBuilder != null){
            JobFlowNode nextJobFlowNode = nextJobFlowNodeBuilder.build(jobFlow);
            this.jobFlowNode.setNextJobFlowNode(nextJobFlowNode);
        }
        return parrelJobFlowNode;

    }

}
