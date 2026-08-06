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
import org.frameworkset.tran.jobflow.ParrelJobFlowNode;
import org.frameworkset.tran.jobflow.context.JobFlowNodeExecuteContext;

/**
 * 
 * @author biaoping.yin
 * @Date 2025/3/31
 */
public class ParrelJobFlowNodeBuilder extends CompositionJobFlowNodeBuilder<ParrelJobFlowNodeBuilder> {
    
    
    
    public ParrelJobFlowNodeBuilder(String nodeId,String nodeName ){
        super( nodeId, nodeName,JobFlowNodeType.PARREL);
    }

    public ParrelJobFlowNodeBuilder(String nodeName ){
        super( null, nodeName,JobFlowNodeType.PARREL);
    }

    public ParrelJobFlowNodeBuilder( ){
        super( JobFlowNodeType.PARREL);
    }

    protected ParrelJobFlowNode buildParrelJobFlowNode(){
        return new ParrelJobFlowNode();
    }
	
	protected void buildDanymicNodes(JobFlowNodeExecuteContext jobFlowNodeExecuteContext){
		
		if(dynamicNodeBuilders != null && dynamicNodeBuilders.size() > 0) {
			ParrelJobFlowNode parrelJobFlowNode = (ParrelJobFlowNode)this.jobFlowNode;
			for (JobFlowNodeBuilder jobFlowNodeBuilder : dynamicNodeBuilders) {
				parrelJobFlowNode.addJobFlowNode(jobFlowNodeBuilder.buildWrapper(parrelJobFlowNode.getJobFlow()));
			}
			
		}
	}
    @Override
    public JobFlowNode build(JobFlow jobFlow){
        if(this.jobFlowNode != null){
            return jobFlowNode;
        }
        ParrelJobFlowNode parrelJobFlowNode = buildParrelJobFlowNode();
		parrelJobFlowNode.setCompositionJobFlowNodeBuilder(this);
        parrelJobFlowNode.setNodeId(this.getNodeId());
        parrelJobFlowNode.setNodeName(this.getNodeName());
        parrelJobFlowNode.setJobFlow(jobFlow);
        if(this.parentJobFlowNodeBuilder != null) {
            parrelJobFlowNode.setParentJobFlowNode(parentJobFlowNodeBuilder.getJobFlowNode());
        }

        if(this.nodeTrigger != null){
            parrelJobFlowNode.setNodeTrigger(nodeTrigger);
        }
        else if(this.nodeTriggerCreate != null){
            parrelJobFlowNode.setNodeTrigger(this.nodeTriggerCreate.createNodeTrigger(this));
        }
        if(nodeBuilders != null && nodeBuilders.size() > 0) {
			for (JobFlowNodeBuilder jobFlowNodeBuilder : nodeBuilders) {
				parrelJobFlowNode.addJobFlowNode(jobFlowNodeBuilder.buildWrapper(jobFlow));
			}
		}
        this.jobFlowNode = parrelJobFlowNode;
        if(this.nextJobFlowNodeBuilder != null){
            JobFlowNode nextJobFlowNode = nextJobFlowNodeBuilder.buildWrapper(jobFlow);
            this.jobFlowNode.setNextJobFlowNode(nextJobFlowNode);
        }
        parrelJobFlowNode.setJobFlowNodeListeners(this.jobFlowNodeListeners);
        return parrelJobFlowNode;

    }

}
