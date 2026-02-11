package org.frameworkset.tran.jobflow.context;
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
import org.frameworkset.tran.jobflow.JobFlowNodeStatus;
import org.frameworkset.tran.jobflow.JobFlowStatus;

/**
 * @author biaoping.yin
 * @Date 2025/6/20
 */
public class NodeTriggerContextImpl implements NodeTriggerContext{
    private JobFlowNodeStatus preJobFlowNodeStatus;
    private StaticContext preJobFlowStaticContext;
    private StaticContext jobFlowStaticContext ;
    private JobFlowExecuteContext jobFlowExecuteContext;
    private JobFlowStatus jobFlowStatus;
    private JobFlow jobFlow;
    private JobFlowNode jobFlowNode;
    private JobFlowNode preFlowNode;
    public NodeTriggerContextImpl(JobFlowNode jobFlowNode,                                   
                                  JobFlow jobFlow){
        this.jobFlowStaticContext = jobFlow.getJobFlowContext().copy();
        this.jobFlowStatus = jobFlow.getJobFlowContext().getJobFlowStatus();
        this.jobFlowExecuteContext = jobFlow.getJobFlowExecuteContext();
        this.jobFlow = jobFlow;
        this.jobFlowNode = jobFlowNode;
        this.preFlowNode = jobFlowNode.getParentJobFlowNode();
        if(preFlowNode != null) {
            this.preJobFlowStaticContext = preFlowNode.getJobFlowNodeContext().copy();
            this.preJobFlowNodeStatus = preFlowNode.getJobFlowNodeContext().getJobFlowNodeStatus();
        }
    }

    @Override
    public StaticContext getJobFlowStaticContext() {
        return jobFlowStaticContext;
    }

    @Override
    public JobFlowExecuteContext getJobFlowExecuteContext() {
        return jobFlowExecuteContext;
    }

    @Override
    public Object getFlowContextData(String name) {
        return jobFlowExecuteContext.getContextData( name);
    }

    @Override
    public Object getFlowContextData(String name,Object defaultValue) {
        return jobFlowExecuteContext.getContextData( name,defaultValue);
    }


    @Override
    public Object getContainerContextData(String name) {
        if(this.jobFlowNode.getContainerJobFlowNodeExecuteContext() != null)
            return this.jobFlowNode.getContainerJobFlowNodeExecuteContext().getContextData( name);
        return null;
    }

    @Override
    public Object getContainerContextData(String name,Object defaultValue) {
        if(this.jobFlowNode.getContainerJobFlowNodeExecuteContext() != null)
            return this.jobFlowNode.getContainerJobFlowNodeExecuteContext().getContextData( name,defaultValue);
        return null;
    }

    @Override
    public Object getContainerContextData(String name,boolean scanParant) {
        if(this.jobFlowNode.getContainerJobFlowNodeExecuteContext() != null)
            return this.jobFlowNode.getContainerJobFlowNodeExecuteContext().getContextData( name,scanParant);
        return null;
    }

    @Override
    public Object getContainerContextData(String name,Object defaultValue,boolean scanParant) {
        if(this.jobFlowNode.getContainerJobFlowNodeExecuteContext() != null)
            return this.jobFlowNode.getContainerJobFlowNodeExecuteContext().getContextData( name,defaultValue,  scanParant);
        return null;
    }

 

    @Override
    public JobFlowStatus getJobFlowStatus() {
        return jobFlowStatus;
    }

    @Override
    public JobFlowNode getRunningJobFlowNode() {
        return jobFlow.getJobFlowContext().getRunningJobFlowNode();
    }

    @Override
    public StaticContext getPreJobFlowStaticContext() {
        return preJobFlowStaticContext;
    }

    @Override
    public JobFlowNode getJobFlowNode() {
        return jobFlowNode;
    }

    @Override
    public JobFlowNodeStatus getPreJobFlowNodeStatus() {
        return preJobFlowNodeStatus;
    }
}
