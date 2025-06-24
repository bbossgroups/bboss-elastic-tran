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

import org.frameworkset.tran.jobflow.JobFlowNode;

import java.util.ArrayList;
import java.util.List;

/**
 * 节点运行情况统计上下文
 * @author biaoping.yin
 * @Date 2025/6/18
 */
public class StaticContext {
    /**
     * 正在运行节点数据
     */
    private int runningNodes;
    /**
     * 启动节点数
     */
    private int startNodes ;
    private List<String> startNodeIds;
    /**
     * 总完成节点数
     * completeSuccessNodes + completeFailedNodes = completeNodes
     */
    private int completeNodes;
    
    private int completeSuccessNodes;
    private int completeWithExceptionNodes;

    private List<String> completeNodeIds;

    private List<String> completeSuccessNodeIds;
    private List<String> completeWithExceptionNodeIds;
    protected Throwable executeException;
    private Object updateLock = new Object();
    public StaticContext(){
        
    }
    public void setExecuteException(Throwable executeException) {
        this.executeException = executeException;
    }

    public Throwable getExecuteException() {
        return executeException;
    }
     
    /**
     * 拷贝节点状态信息
     * @return
     */
    public StaticContext copy(){
        synchronized (updateLock) {
            StaticContext staticContext = new StaticContext();
            staticContext.startNodes = this.startNodes;
            staticContext.completeNodes = this.completeNodes;
            staticContext.runningNodes = this.runningNodes;
            staticContext.completeWithExceptionNodes = this.completeWithExceptionNodes;
            staticContext.completeSuccessNodes = this.completeSuccessNodes;
            staticContext.completeNodeIds = this.completeNodeIds;
            staticContext.startNodeIds = this.startNodeIds;
            staticContext.completeSuccessNodeIds = this.completeSuccessNodeIds;
            staticContext.completeWithExceptionNodeIds = this.completeWithExceptionNodeIds;
            staticContext.executeException = this.executeException;
            return staticContext;
        }
    }

    /**
     * 重置计数器
     */
    public void reset() {
        synchronized (updateLock) {
            startNodes = 0;
            completeNodes = 0;
            runningNodes = 0;
            completeWithExceptionNodes = 0;
            completeSuccessNodes = 0;
            completeNodeIds = null;
            startNodeIds = null;
            completeSuccessNodeIds = null;
            completeWithExceptionNodeIds = null;
        }
        executeException = null;
    }
    
    

    /**
     * 获取所有已经启动的节点数量
     * @return
     */
    public int getStartNodes() {
        synchronized (updateLock) {
            return startNodes;
        }
    }

    /**
     * 节点启动时，增加启动节点计数
     * @return
     */
    public  int nodeStart(JobFlowNode jobFlowNode) {
        synchronized (updateLock) {
            startNodes ++;
            if(this.startNodeIds == null){
                startNodeIds = new ArrayList<>();
            }
            startNodeIds.add(jobFlowNode.getNodeId());
            runningNodes ++;
            return startNodes;
        }
    }

    /**
     * 节点完成时，减少启动节点计数,完成计数器加1
     * @param throwable 子节点触发的异常
     * @param jobFlowNode 完成的子节点
     * @return
     */
    public int nodeComplete(Throwable throwable, JobFlowNode jobFlowNode) {
        synchronized (updateLock) {
//            executeException = throwable;
            runningNodes --;//正在运行节点数量递减
            completeNodes++;
            if(this.completeNodeIds == null){
                completeNodeIds = new ArrayList<>();
            }
            completeNodeIds.add(jobFlowNode.getNodeId());
             
            if(throwable == null){
                completeSuccessNodes ++;
                if(this.completeSuccessNodeIds == null){
                    completeSuccessNodeIds = new ArrayList<>();
                }
                completeSuccessNodeIds.add(jobFlowNode.getNodeId());
            }
            else{
                if(this.completeWithExceptionNodeIds == null){
                    completeWithExceptionNodeIds = new ArrayList<>();
                }
                completeWithExceptionNodeIds.add(jobFlowNode.getNodeId());
                completeWithExceptionNodes++;
            }
            return completeNodes;
        }
    }

    /**
     * 获取运行中的节点
     * @return
     */
    public int getRunningNodes(){
        synchronized (updateLock) {
            return runningNodes;
        }
    }

    /**
     * 获取已经执行完成的节点
     * @return
     */
    public int getCompleteNodes(){
        synchronized (updateLock) {
            return completeNodes;
        }
    }

    public int getCompleteWithExceptionNodes() {
        synchronized (updateLock) {
            return completeWithExceptionNodes;
        }
    }

    public int getCompleteSuccessNodes() {
        synchronized (updateLock) {
            return completeSuccessNodes;
        }
    }

    /**
     * 判断所有启动的节点是否都已经执行完成
     * @return
     */
    public boolean allNodeComplete(){
        synchronized (updateLock) {
            return completeNodes == startNodes;
        }
    }

    public List<String> getCompleteWithExceptionNodeIds() {
        synchronized (updateLock) {
            return completeWithExceptionNodeIds;
        }
    }

    public List<String> getCompleteNodeIds() {
        synchronized (updateLock) {
            return completeNodeIds;
        }
    }

    public List<String> getCompleteSuccessNodeIds() {
        synchronized (updateLock) {
            return completeSuccessNodeIds;
        }
    }

    public List<String> getStartNodeIds() {
        synchronized (updateLock) {
            return startNodeIds;
        }
    }
}
