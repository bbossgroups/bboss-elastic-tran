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
import org.frameworkset.tran.jobflow.context.JobFlowNodeExecuteContext;

import java.util.ArrayList;
import java.util.List;

/**
 * 复合类型流程节点：构成并行分支的串行分支节点和并行分支节点
 * @author biaoping.yin
 * @Date 2025/3/31
 */
public abstract class CompositionJobFlowNodeBuilder<T extends CompositionJobFlowNodeBuilder> extends JobFlowNodeBuilder<T> {
    
     
    private JobFlowNodeType jobFlowNodeType ; 
    /**
     * 并行节点作业配置清单-定义是添加
     */
    protected List<JobFlowNodeBuilder> nodeBuilders;
	
	/**
	 * 并行节点作业配置-动态添加
	 */
	protected List<JobFlowNodeBuilder> dynamicNodeBuilders;
	
	protected DynamicNodeBuilder dynamicNodeBuilder;
	
	
	/**
	 * 并行节点作业配置-动态添加
	 */
	protected List<JobFlowNodeBuilder> dynamicRemovedNodeBuilders;
	
	
	
	
	public CompositionJobFlowNodeBuilder(String nodeId,String nodeName,JobFlowNodeType jobFlowNodeType){
        super(nodeId,nodeName);
        this.jobFlowNodeType = jobFlowNodeType;
    }

    public CompositionJobFlowNodeBuilder(JobFlowNodeType jobFlowNodeType){
        super(null,null);
        this.jobFlowNodeType = jobFlowNodeType;
    }
    public CompositionJobFlowNodeBuilder(){
        super();
        this.jobFlowNodeType = jobFlowNodeType;
    }

    public CompositionJobFlowNodeBuilder(String nodeId,String nodeName){
        super(nodeId,nodeName);
        jobFlowNodeType = JobFlowNodeType.SEQUENCE;
    }
	
	public void setDynamicNodeBuilder(DynamicNodeBuilder dynamicNodeBuilder) {
		this.dynamicNodeBuilder = dynamicNodeBuilder;
	}
	
	public DynamicNodeBuilder getDynamicNodeBuilder() {
		return dynamicNodeBuilder;
	}
	
	protected void init(){
        if(nodeBuilders == null){
            nodeBuilders = new ArrayList<>();
        }
		
    }

    protected void validate(JobFlowNodeBuilder jobFlowNodeBuilder){
        if(jobFlowNodeBuilder.getCompositionJobFlowNodeBuilder() != null){
            if(!(jobFlowNodeBuilder.getCompositionJobFlowNodeBuilder() instanceof ConditionJobFlowNodeBuilder))
                throw new JobFlowBuilderException("节点不能重复添加到复合节点中");
        }
    }

	protected void buildDanymicNodes(JobFlowNodeExecuteContext jobFlowNodeExecuteContext){
		
	}
	public void handleDynamicNodeBuilders(JobFlowNodeExecuteContext jobFlowNodeExecuteContext){
		if(builded) {
			if(this.dynamicNodeBuilder != null){
				dynamicNodeBuilder.nodeBuilder(jobFlowNodeExecuteContext);
			}
			if (dynamicNodeBuilders != null && dynamicNodeBuilders.size() > 0) {
			 
				buildDanymicNodes(jobFlowNodeExecuteContext);
				/**
				 * 标记工作流节点是否已经构建过
				 * 主要用于工作流中节点动态调整，添加或者移除节点
				 */
				dynamicBuilded = true;
			}
		}
	}
    /**
     * 添加复杂并行子任务，存在串行多个子任务或者串行任务
     * @param jobFlowNodeBuilder
     * @return
     */
    public T addJobFlowNodeBuilder(JobFlowNodeBuilder jobFlowNodeBuilder){
//        this.nodeBuilder = importBuilderCreate.createImportBuilder(this);
        init();
        validate(jobFlowNodeBuilder);
        if(jobFlowNodeBuilder.getCompositionJobFlowNodeBuilder() == null) {
            jobFlowNodeBuilder.setCompositionJobFlowNodeBuilder(this);
        }
		if(!this.builded) {
			nodeBuilders.add(jobFlowNodeBuilder);
		}
		else{
			if(dynamicNodeBuilders == null || dynamicBuilded){
				dynamicNodeBuilders = new ArrayList<>();
				dynamicBuilded = false;
			}
			dynamicNodeBuilders.add(jobFlowNodeBuilder);
		}
        return (T)this;
    }


    public T setJobFlowNodeType(JobFlowNodeType jobFlowNodeType) {
        if(jobFlowNodeType == JobFlowNodeType.SIMPLE){
            throw new JobFlowException("CompositionJobFlowNodeBuilder jobFlowNodeType must be set SEQUENCE or PARREL,but is SIMPLE.");
        }
        this.jobFlowNodeType = jobFlowNodeType;
        return (T)this;
    }

    public JobFlowNodeType getJobFlowNodeType() {
        return jobFlowNodeType;
    }
}
