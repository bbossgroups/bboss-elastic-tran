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
import org.frameworkset.tran.jobflow.JobFlowException;
import org.frameworkset.tran.jobflow.JobFlowNode;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 节点内部子节点之间传递和共享参数
 * @author biaoping.yin
 * @Date 2025/3/31
 */
public class DefaultJobFlowNodeExecuteContext implements JobFlowNodeExecuteContext{
    private Map<String,Object> contextDatas = new LinkedHashMap<>();
    private JobFlowNode jobFlowNode;
    private JobFlow jobFlow;
   
    /**
     * 判断节点是否已经完成标记
     */
    private AtomicBoolean nodeCompleteExecuted = new AtomicBoolean(false);
    
    
    
    public DefaultJobFlowNodeExecuteContext(JobFlowNode jobFlowNode){
        this.jobFlowNode = jobFlowNode;
        this.jobFlow = jobFlowNode.getJobFlow();
        
    }
    /**
     * 判断节点是否已经完成
     */
    public boolean nodeCompleteUnExecuted(){
        return nodeCompleteExecuted.compareAndSet(false, true);
    }
    @Override
    public synchronized Object getContextData(String name) {
        return getContextData(  name,false);
    }

    @Override
    public synchronized Object getContextData(String name, Object defaultValue) {
        Object value = contextDatas.get(name);
        if(value == null){
            value = defaultValue;
        }
        return value;
    }

    @Override
    public Object getContextData(String name,boolean fromContainer){
        Object value = contextDatas.get(name);
        if(value == null && fromContainer){
            if(this.getContainerJobFlowNodeExecuteContext() != null) {
                value = this.getContainerJobFlowNodeExecuteContext().getContextData(name,true);
            }
        }
        return value;
    }
    @Override
    public Object getContextData(String name,Object defaultValue,boolean fromContainer){
        Object value = contextDatas.get(name);
        
        if(value == null && fromContainer){
            if(this.getContainerJobFlowNodeExecuteContext() != null) {
                value = this.getContainerJobFlowNodeExecuteContext().getContextData(name,true);
            }
        }
        if(value == null){
            value = defaultValue;
        }
        return value;
    }

    @Override
    public Object getJobFlowContextData(String name) {
        return this.getJobFlowExecuteContext().getContextData(name);
    }

    @Override
    public Object getJobFlowContextData(String name, Object defaultValue) {
        return this.getJobFlowExecuteContext().getContextData(name,defaultValue);
    }

    @Override
    public Object getContainerJobFlowNodeContextData(String name) {
        if(this.getContainerJobFlowNodeExecuteContext() != null) {
            return this.getContainerJobFlowNodeExecuteContext().getContextData(name);
        }
        return null;
    }

    @Override
    public Object getContainerJobFlowNodeContextData(String name, Object defaultValue) {
        if(this.getContainerJobFlowNodeExecuteContext() != null) {
            return this.getContainerJobFlowNodeExecuteContext().getContextData(name,defaultValue);
        }
        return null;
    }


    /**
     * 获取容器节点对应的上下文数据,scanParent为true时,会逐级递从当前容器到上级容器递归获取上下文参数数据，直到获取为止或者达到最上级为止
     * @param name
     *  @param scanParent true 遍历上级容器,会逐级递从当前容器到上级容器递归获取上下文参数数据，直到获取为止或者达到最上级为止 false不遍历
     * @return
     */
    @Override
    public Object getContainerJobFlowNodeContextData(String name,boolean scanParent){
        if(this.getContainerJobFlowNodeExecuteContext() != null) {
            return this.getContainerJobFlowNodeExecuteContext().getContextData(name,scanParent);
        }
        return null;
    }
    

    /**
     * 获取容器节点对应的上下文数据,scanParent为true时,会逐级递从当前容器到上级容器递归获取上下文参数数据，直到获取为止或者达到最上级为止
     * 如果最终参数值为null，则返回默认值
     * @param name
     * @param defaultValue
     * @param scanParent true 遍历上级容器,会逐级递从当前容器到上级容器递归获取上下文参数数据，直到获取为止或者达到最上级为止 false不遍历
     * @return
     */
    @Override
    public Object getContainerJobFlowNodeContextData(String name,Object defaultValue,boolean scanParent){
        if(this.getContainerJobFlowNodeExecuteContext() != null) {
            return this.getContainerJobFlowNodeExecuteContext().getContextData(name,defaultValue,scanParent);
        }
        return null;
    }
    


    @Override
    public synchronized void putAll(Map<String,Object> contextDatas){
        this.contextDatas.putAll(contextDatas);
    }

    @Override
    public synchronized void addContextData(String name, Object data) {
        this.contextDatas.put(name,data);
    }

    @Override
    public void addJobFlowContextData(String name, Object data) {
        getJobFlowExecuteContext().addContextData(name,data);
    }

    @Override
    public void addContainerJobFlowNodeContextData(String name, Object data) {
        if(this.getContainerJobFlowNodeExecuteContext() != null) {
            this.getContainerJobFlowNodeExecuteContext().addContextData(name,data);
        }
        throw new JobFlowException("addContainerJobFlowNodeContextData failed:ContainerJobFlowNodeExecuteContext is null.");
    }

    @Override
    public synchronized void clear(){
        this.contextDatas.clear();
    }

    @Override
    public JobFlowExecuteContext getJobFlowExecuteContext() {
        return this.jobFlow.getJobFlowExecuteContext();
    }

    /**
     * 获取子节点对应的复合节点执行上下文
     *
     * @return
     */
    @Override
    public JobFlowNodeExecuteContext getContainerJobFlowNodeExecuteContext() {
        return jobFlowNode.getContainerJobFlowNodeExecuteContext();
    }

    @Override
    public void pauseAwait(){
        this.jobFlow.getJobFlowContext().pauseAwait(jobFlowNode);
    }
    /**
     * 判断作业是否已经停止或者正在停止中
     * @return
     */
    @Override
    public AssertResult assertStopped(){
        return this.jobFlow.getJobFlowContext().assertStopped();
    }

    @Override
    public StaticContext getJobFlowNodeStaticContext() {
        return jobFlowNode.getJobFlowNodeContext().copy();
    }
    
    public String getNodeId(){
        return jobFlowNode.getNodeId();
    }

    public String getNodeName(){
        return jobFlowNode.getNodeName();
    }

    public JobFlowNode getJobFlowNode() {
        return jobFlowNode;
    }
    public JobFlow getJobFlow() {
        return jobFlow;
    }
}
