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

import java.util.Map;

/**
 * <p>Description: </p>
 * <p></p>
 *
 * @author biaoping.yin
 * @Date 2025/3/31
 */
public interface JobFlowNodeExecuteContext {
    Object getContextData(String name);

    Object getContextData(String name,Object defaultValue);

    Object getContextData(String name,boolean fromContainer);

    Object getContextData(String name,Object defaultValue,boolean fromContainer);
    
    Object getJobFlowContextData(String name);

    Object getJobFlowContextData(String name,Object defaultValue);
    /**
     * 获取容器节点对应的上下文数据,会逐级递从当前容器到上级容器递归获取上下文参数数据，直到获取为止或者达到最上级为止
     * @param name
     * @return
     */
    Object getContainerJobFlowNodeContextData(String name);

    /**
     * 获取容器节点对应的上下文数据,会逐级递从当前容器到上级容器递归获取上下文参数数据，直到获取为止或者达到最上级为止
     * 如果最终参数值为null，则返回默认值
     * @param name
     * @param defaultValue
     * @return
     */
    Object getContainerJobFlowNodeContextData(String name,Object defaultValue);
    
    /**
     * 判断节点是否已经完成
     */
    boolean nodeCompleteUnExecuted();

    void putAll(Map<String, Object> contextDatas);

    void addContextData(String name,Object data);

    void addJobFlowContextData(String name,Object data);

    void addContainerJobFlowNodeContextData(String name,Object data);

    void clear();
    JobFlowExecuteContext getJobFlowExecuteContext();

    /**
     * 获取子节点对应的复合节点执行上下文 
     * @return
     */
    JobFlowNodeExecuteContext getContainerJobFlowNodeExecuteContext();

    /**
     * 暂停节点
     */
    void pauseAwait();
    /**
     * 判断作业是否已经停止或者正在停止中
     * @return
     */
    AssertResult assertStopped();

    StaticContext getJobFlowNodeStaticContext();

    String getNodeId() ;
       

    String getNodeName();

    

    JobFlowNode getJobFlowNode() ;
    JobFlow getJobFlow() ;
}
