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
import org.frameworkset.tran.jobflow.JobFlowNodeFunction;
import org.frameworkset.tran.jobflow.SimpleJobFlowNode;

/**
 * 通用工作流作业节点
 * @author biaoping.yin
 * @Date 2025/3/31
 */
public abstract class SimpleJobFlowNodeBuilder<T extends SimpleJobFlowNodeBuilder> extends JobFlowNodeBuilder<T> {

    /**
     * 默认作业节点在Function call结束后都会自动结束，如果需要手动在Function中的call方法调用nodeComplete，则将autoNodeComplete设置为false
     * 目前框架默认提供的数据交换节点需要手动在数据交换作业完成时结束作业节点
     * 注意：同一个节点在一次调度执行过程中，nodeComplete只会被调用一次，并且确保被调用一次
     */
    protected boolean autoNodeComplete = true;

    public SimpleJobFlowNodeBuilder(String nodeId, String nodeName) {
        super(nodeId, nodeName);
    }
    /**
     * 串行节点作业配置
     */
//    protected ImportBuilder importBuilder;

    public SimpleJobFlowNodeBuilder() {
        super();        
    }
    /**
     * 默认作业节点在Function call结束后都会自动结束，如果需要手动在Function中的call方法调用nodeComplete，则将autoNodeComplete设置为false
     * 目前框架默认提供的数据交换节点需要手动在数据交换作业完成时结束作业节点
     * 注意：同一个节点在一次调度执行过程中，nodeComplete只会被调用一次，并且确保被调用一次
     */
    public T setAutoNodeComplete(boolean autoNodeComplete) {
        this.autoNodeComplete = autoNodeComplete;
        return (T)this;
    }
    /**
     * 默认作业节点在Function call结束后都会自动结束，如果需要手动在Function中的call方法调用nodeComplete，则将autoNodeComplete设置为false
     * 目前框架默认提供的数据交换节点需要手动在数据交换作业完成时结束作业节点
     * 注意：同一个节点在一次调度执行过程中，nodeComplete只会被调用一次，并且确保被调用一次
     */
    public boolean isAutoNodeComplete() {
        return autoNodeComplete;
    }

    protected abstract JobFlowNodeFunction buildJobFlowNodeFunction();
    
    @Override
    public JobFlowNode build(JobFlow jobFlow){
        if(this.jobFlowNode != null){
            return jobFlowNode;
        }
        SimpleJobFlowNode simpleJobFlowNode = null;       
        
        simpleJobFlowNode = new SimpleJobFlowNode(buildJobFlowNodeFunction(),nodeTrigger); 
        simpleJobFlowNode.setNodeId(this.getNodeId());
        simpleJobFlowNode.setNodeName(this.getNodeName());
        simpleJobFlowNode.setJobFlow(jobFlow);
        simpleJobFlowNode.setAutoNodeComplete(this.autoNodeComplete);
        if(this.parentJobFlowNodeBuilder != null) {
            simpleJobFlowNode.setParentJobFlowNode(parentJobFlowNodeBuilder.getJobFlowNode());
        }
       
        this.jobFlowNode = simpleJobFlowNode;
        if(this.nextJobFlowNodeBuilder != null){
            JobFlowNode nextJobFlowNode = nextJobFlowNodeBuilder.build(jobFlow);
            this.jobFlowNode.setNextJobFlowNode(nextJobFlowNode);
        }        
        simpleJobFlowNode.setJobFlowNodeListeners(this.jobFlowNodeListeners);
        return simpleJobFlowNode;

       
        
    }




}
