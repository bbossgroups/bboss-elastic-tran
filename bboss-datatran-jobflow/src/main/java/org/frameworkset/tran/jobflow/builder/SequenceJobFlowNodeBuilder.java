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

//    /**
//     *
//     * @param nodeId
//     * @param nodeName
//     * @param importBuilder
//     * @param nodeTrigger
//     * @return
//     */
//    public SequenceJobFlowNodeBuilder addImportBuilder(String nodeId, String nodeName, ImportBuilder importBuilder, NodeTrigger nodeTrigger){
//        JobFlowNodeBuilder jobFlowNodeBuilder = new SimpleJobFlowNodeBuilder().setImportBuilder(importBuilder).setNodeTrigger(nodeTrigger)
//                .setNodeId(nodeId).setNodeName(nodeName);
//        _addJobFlowNodeBuilder(  jobFlowNodeBuilder);
//        
//        
//        return this;
//    }
//
//    /**
//     * 添加一个简单的子任务
//     * @param nodeId
//     * @param nodeName
//     * @param importBuilderCreate
//     * @return
//     */
//    public SequenceJobFlowNodeBuilder addImportBuilder(String nodeId, String nodeName, ImportBuilderCreate importBuilderCreate){
//        JobFlowNodeBuilder jobFlowNodeBuilder = new SimpleJobFlowNodeBuilder().buildImportBuilder(importBuilderCreate)
//                .setNodeId(nodeId).setNodeName(nodeName);
//        _addJobFlowNodeBuilder(  jobFlowNodeBuilder);
//        return this;
//    }
//
//    /**
//     * 添加一个简单的子任务
//     * @param nodeId
//     * @param nodeName
//     * @param importBuilder
//     * @return
//     */
//    public SequenceJobFlowNodeBuilder addImportBuilder(String nodeId,String nodeName,ImportBuilder importBuilder){
//        JobFlowNodeBuilder jobFlowNodeBuilder = new SimpleJobFlowNodeBuilder().setImportBuilder(importBuilder)
//                .setNodeId(nodeId).setNodeName(nodeName);
//        _addJobFlowNodeBuilder(  jobFlowNodeBuilder);
//        return this;
//    }

    /**
     * 添加复杂串行子任务，存在串行多个子任务或者串行任务
     * @param jobFlowNodeBuilder
     * @return
     */
    public SequenceJobFlowNodeBuilder addJobFlowNodeBuilder(JobFlowNodeBuilder jobFlowNodeBuilder){
        init();

        if(this.headerJobFlowNodeBuilder == null) {
            this.headerJobFlowNodeBuilder = jobFlowNodeBuilder;

        }
        if(currentJobFlowNodeBuilder != null)
            this.currentJobFlowNodeBuilder.setNextJobFlowNodeBuilder(jobFlowNodeBuilder);
        this.currentJobFlowNodeBuilder = jobFlowNodeBuilder;
        nodeBuilders.add(jobFlowNodeBuilder);
        return this;
    }

//    /**
//     * 添加复杂并行子任务，存在串行多个子任务或者串行任务
//     * @param jobFlowNodeBuilder
//     * @return
//     */
//    public SequenceJobFlowNodeBuilder addJobFlowNodeBuilder(CompositionJobFlowNodeBuilder jobFlowNodeBuilder){
//        _addJobFlowNodeBuilder(  jobFlowNodeBuilder);
//        return this;
//    }


    @Override
    public JobFlowNode build(JobFlow jobFlow){
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
//        for(JobFlowNodeBuilder jobFlowNodeBuilder:nodeBuilders){
//            compositionJobFlowNode.addJobFlowNode(jobFlowNodeBuilder.build(jobFlow));
//        }
        this.jobFlowNode = sequenceJobFlowNode;
        if(this.nextJobFlowNodeBuilder != null){
            JobFlowNode nextJobFlowNode = nextJobFlowNodeBuilder.build(jobFlow);
            this.jobFlowNode.setNextJobFlowNode(nextJobFlowNode);
        }

        sequenceJobFlowNode.setJobFlowNodeListeners(this.jobFlowNodeListeners);
        return sequenceJobFlowNode;

    }


}
