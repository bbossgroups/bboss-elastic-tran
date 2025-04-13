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

import org.frameworkset.tran.config.ImportBuilder;

import java.util.ArrayList;
import java.util.List;

/**
 * <p>Description: </p>
 * <p></p>
 *
 * @author biaoping.yin
 * @Date 2025/3/31
 */
public class ParrelJobFlowNodeBuilder extends JobFlowNodeBuilder{
    
     
    /**
     * 并行节点作业配置
     */
    private List<JobFlowNodeBuilder> nodeBuilders;


    private void init(){
        if(nodeBuilders == null){
            nodeBuilders = new ArrayList<>();
        }
    }


    /**
     * 设置并行节点的总触发器构建器，如果总触发器不成立，在不执行nodeBuilders中的所有子任务，每个子任务都可以有自己的触发器
     * @param parrelNodeTriggerCreate
     * @return
     */
    public ParrelJobFlowNodeBuilder addParrelNodeTriggerCreate(NodeTriggerCreate parrelNodeTriggerCreate){
        this.nodeTriggerCreate = parrelNodeTriggerCreate;
        return this;
    }
    /**
     * 添加一个简单的子任务,并设置任务触发器
     * @param importBuilderCreate
     * @param nodeTriggerCreate
     * @return
     */
    public ParrelJobFlowNodeBuilder addImportBuilder(ImportBuilderCreate importBuilderCreate,NodeTriggerCreate nodeTriggerCreate){
//        this.nodeBuilder = importBuilderCreate.createImportBuilder(this);
        init();
        nodeBuilders.add(new ComJobFlowNodeBuilder().buildImportBuilder(importBuilderCreate,nodeTriggerCreate));
        return this;
    }

    /**
     * 添加一个简单的子任务
     * @param importBuilderCreate
     * @return
     */
    public ParrelJobFlowNodeBuilder addImportBuilder(ImportBuilderCreate importBuilderCreate){
//        this.nodeBuilder = importBuilderCreate.createImportBuilder(this);
        init();
        nodeBuilders.add(new ComJobFlowNodeBuilder().buildImportBuilder(importBuilderCreate));
        return this;
    }

    /**
     * 添加复杂并行子任务，存在串行多个子任务或者串行任务
     * @param jobFlowNodeBuilder
     * @return
     */
    public ParrelJobFlowNodeBuilder addJobFlowNodeBuilder(JobFlowNodeBuilder jobFlowNodeBuilder){
//        this.nodeBuilder = importBuilderCreate.createImportBuilder(this);
        init();
        nodeBuilders.add(jobFlowNodeBuilder);
        return this;
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
