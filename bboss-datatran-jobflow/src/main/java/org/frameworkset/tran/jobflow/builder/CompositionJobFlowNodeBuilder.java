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

import java.util.ArrayList;
import java.util.List;

/**
 * 复合类型流程节点：构成并行分支的串行分支节点和并行分支节点
 * @author biaoping.yin
 * @Date 2025/3/31
 */
public abstract class CompositionJobFlowNodeBuilder extends JobFlowNodeBuilder {
    
     
    private JobFlowNodeType jobFlowNodeType ; 
    /**
     * 并行节点作业配置
     */
    protected List<JobFlowNodeBuilder> nodeBuilders;
     
    public CompositionJobFlowNodeBuilder(String nodeId,String nodeName,JobFlowNodeType jobFlowNodeType){
        super(nodeId,nodeName);
        this.jobFlowNodeType = jobFlowNodeType;
    }

    public CompositionJobFlowNodeBuilder(JobFlowNodeType jobFlowNodeType){
        this.jobFlowNodeType = jobFlowNodeType;
    }
    public CompositionJobFlowNodeBuilder(){
        
        this.jobFlowNodeType = jobFlowNodeType;
    }

    public CompositionJobFlowNodeBuilder(String nodeId,String nodeName){
        super(nodeId,nodeName);
        jobFlowNodeType = JobFlowNodeType.SEQUENCE;
    }



    protected void init(){
        if(nodeBuilders == null){
            nodeBuilders = new ArrayList<>();
        }
    }


//    /**
//     * 设置复合节点的总触发器构建器，如果总触发器不成立，在不执行nodeBuilders中的所有子任务，每个子任务都可以有自己的触发器
//     * @param compositionNodeTriggerCreate
//     * @return
//     */
//    public CompositionJobFlowNodeBuilder addNodeTriggerCreate(NodeTriggerCreate compositionNodeTriggerCreate){
//        this.nodeTriggerCreate = compositionNodeTriggerCreate;
//        return this;
//    }
//    /**
//     * 添加一个简单的子任务,并设置任务触发器
//     * @param nodeId 
//     * @param nodeName 
//     * @param importBuilderCreate
//     * @param nodeTriggerCreate
//     * @return
//     */
//    public CompositionJobFlowNodeBuilder addImportBuilder(String nodeId, String nodeName, ImportBuilderCreate importBuilderCreate, NodeTriggerCreate nodeTriggerCreate){
////        this.nodeBuilder = importBuilderCreate.createImportBuilder(this);
//        init();
//        nodeBuilders.add(new SimpleJobFlowNodeBuilder().buildImportBuilder(importBuilderCreate,nodeTriggerCreate)
//                .setNodeId(nodeId).setNodeName(nodeName));
//        return this;
//    }
//
//    /**
//     * 
//     * @param nodeId
//     * @param nodeName
//     * @param importBuilder
//     * @param nodeTrigger
//     * @return
//     */
//    public CompositionJobFlowNodeBuilder addImportBuilder(String nodeId,String nodeName,ImportBuilder importBuilder, NodeTrigger nodeTrigger){
////        this.nodeBuilder = importBuilderCreate.createImportBuilder(this);
//        init();
//        nodeBuilders.add(new SimpleJobFlowNodeBuilder().setImportBuilder(importBuilder).setNodeTrigger(nodeTrigger)
//                .setNodeId(nodeId).setNodeName(nodeName));
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
//    public CompositionJobFlowNodeBuilder addImportBuilder(String nodeId,String nodeName,ImportBuilderCreate importBuilderCreate){
////        this.nodeBuilder = importBuilderCreate.createImportBuilder(this);
//        init();
//        nodeBuilders.add(new SimpleJobFlowNodeBuilder().buildImportBuilder(importBuilderCreate)
//                .setNodeId(nodeId).setNodeName(nodeName));
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
//    public CompositionJobFlowNodeBuilder addImportBuilder(String nodeId,String nodeName,ImportBuilder importBuilder){
////        this.nodeBuilder = importBuilderCreate.createImportBuilder(this);
//        init();
//        nodeBuilders.add(new SimpleJobFlowNodeBuilder().setImportBuilder(importBuilder)
//                .setNodeId(nodeId).setNodeName(nodeName));
//        return this;
//    }

    /**
     * 添加复杂并行子任务，存在串行多个子任务或者串行任务
     * @param jobFlowNodeBuilder
     * @return
     */
    public CompositionJobFlowNodeBuilder addJobFlowNodeBuilder(JobFlowNodeBuilder jobFlowNodeBuilder){
//        this.nodeBuilder = importBuilderCreate.createImportBuilder(this);
        init();
        nodeBuilders.add(jobFlowNodeBuilder);
        return this;
    }


    public CompositionJobFlowNodeBuilder setJobFlowNodeType(JobFlowNodeType jobFlowNodeType) {
        if(jobFlowNodeType == JobFlowNodeType.SIMPLE){
            throw new JobFlowException("CompositionJobFlowNodeBuilder jobFlowNodeType must be set SEQUENCE or PARREL,but is SIMPLE.");
        }
        this.jobFlowNodeType = jobFlowNodeType;
        return this;
    }

    public JobFlowNodeType getJobFlowNodeType() {
        return jobFlowNodeType;
    }
}
