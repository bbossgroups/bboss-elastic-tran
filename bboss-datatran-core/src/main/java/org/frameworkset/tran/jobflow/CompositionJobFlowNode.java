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
 * 组合流程节点，可以由多个节点：simple，Sequence，Parrel节点组合的复杂流程分支节点
 * @author biaoping.yin
 * @Date 2025/3/31
 */
public abstract class CompositionJobFlowNode extends JobFlowNode{
    private static Logger logger = LoggerFactory.getLogger(CompositionJobFlowNode.class);
    /**
     * 并行节点作业配置
     */
    protected List<JobFlowNode> jobFlowNodes;
    protected IntegerCount startNodes = null;
 
 
    public void addJobFlowNode(JobFlowNode jobFlowNode){
        if(this.jobFlowNodes == null){
            jobFlowNodes = new ArrayList<>();
        }
        jobFlowNode.setCompositionJobFlowNode(this);
        this.jobFlowNodes.add(jobFlowNode);
    }

    /**
     * 启动流程当前节点
     */
    @Override
    public abstract boolean start();

    /**
     * 停止流程当前节点
     */
    @Override
    public abstract void stop();

    /**
     * 暂停流程节点
     */
    @Override
    public void pause() {
        for (int i = 0; jobFlowNodes != null && i < jobFlowNodes.size(); i++) {
            JobFlowNode jobFlowNode = jobFlowNodes.get(i);
            jobFlowNode.pause();
        }
    }

    /**
     * 唤醒暂停流程节点
     */
    @Override
    public void consume() {
        for (int i = 0; jobFlowNodes != null && i < jobFlowNodes.size(); i++) {
            JobFlowNode jobFlowNode = jobFlowNodes.get(i);
            jobFlowNode.consume();
        }
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
