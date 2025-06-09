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

import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.util.concurrent.IntegerCount;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * <p>Description: </p>
 * <p></p>
 *
 * @author biaoping.yin
 * @Date 2025/3/31
 */
public class ParrelJobFlowNode extends JobFlowNode{
    private static Logger logger = LoggerFactory.getLogger(ParrelJobFlowNode.class);
    /**
     * 并行节点作业配置
     */
    private List<JobFlowNode> jobFlowNodes;
    private IntegerCount startNodes = null;
 
 
    public void addJobFlowNode(JobFlowNode jobFlowNode){
        if(this.jobFlowNodes == null){
            jobFlowNodes = new ArrayList<>();
        }
        jobFlowNode.setParrelJobFlowNode(this);
        this.jobFlowNodes.add(jobFlowNode);
    }

    /**
     * 启动流程当前节点
     */
    @Override
    public boolean start(){
        
        JobFlowExecuteContext jobFlowExecuteContext = jobFlow.getJobFlowExecuteContext();
//        if(parentJobFlowNode == null) {
//           throw new JobFlowException("ParrelJobFlowNode must have a ParrelJobFlowNode,please set ParrelJobFlowNode first.");
//        }
        startNodes = new IntegerCount();
        if(assertTrigger()) {
            if (jobFlowNodes == null || jobFlowNodes.size() == 0) {
                throw new JobFlowException("ParrelJobFlowNode must set jobFlowNodes,please set jobFlowNodes first.");
            } else {
                for (int i = 0; i < jobFlowNodes.size(); i++) {
                    JobFlowNode jobFlowNode = jobFlowNodes.get(i);
                    if(jobFlowNode.start())
                        startNodes.increament();
                }
            }
            return true;
        }
        return false;
        
    }

    /**
     * 停止流程当前节点
     */
    @Override
    public void stop(){
        JobFlowExecuteContext jobFlowExecuteContext = jobFlow.getJobFlowExecuteContext();
    }

    /**
     * 暂停流程节点
     */
    @Override
    public void pause(){
        JobFlowExecuteContext jobFlowExecuteContext = jobFlow.getJobFlowExecuteContext();
    }

    /**
     * 唤醒暂停流程节点
     */
    @Override
    public void consume() {
        JobFlowExecuteContext jobFlowExecuteContext = jobFlow.getJobFlowExecuteContext();
    }

    /**
     * 分支完成
     * @param jobFlowNode
     */
    public void brachComplete(JobFlowNode jobFlowNode, ImportContext importContext, Throwable e) {
        int liveNodes = this.startNodes.decreament();
        if(liveNodes <= 0){
            this.nodeComplete(  importContext,   e);
        }
    }

    /**
     * 分支完成
     * @param jobFlowNode
     */
    public void brachComplete(JobFlowNode jobFlowNode, TaskContext taskContext, Throwable e) {
        int liveNodes = this.startNodes.decreament();
        if(liveNodes <= 0){
            this.nodeComplete(  taskContext,   e);
        }
    }
}
