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

import org.apache.commons.collections.CollectionUtils;
import org.frameworkset.tran.jobflow.context.*;
import org.frameworkset.tran.jobflow.listener.JobFlowNodeListener;
import org.frameworkset.util.concurrent.ThreadPoolFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * 并行任务流程节点
 * @author biaoping.yin
 * @Date 2025/3/31
 */
public class ParrelJobFlowNode extends CompositionJobFlowNode{
    private static Logger logger = LoggerFactory.getLogger(ParrelJobFlowNode.class);
    private ExecutorService blockedExecutor;
    private Object blockedExecutorLock = new Object();
    private ParrelJobFlowNodeContext parrelJobFlowNodeContext;
    public ParrelJobFlowNode(){
        this.jobFlowNodeType = JobFlowNodeType.PARREL;
        this.parrelJobFlowNodeContext = new ParrelJobFlowNodeContext(this);
        this.jobFlowNodeContext = parrelJobFlowNodeContext;
    }
    public ExecutorService buildThreadPool(){
        if(blockedExecutor != null)
            return blockedExecutor;
        synchronized (blockedExecutorLock) {
            if(blockedExecutor == null) {
                blockedExecutor = ThreadPoolFactory.buildThreadPool("ParrelJobFlowNode["+this.nodeName+"]","ParrelJobFlowNode["+this.nodeName+"]",
                        jobFlowNodes.size(),10,
                        -1l
                        ,1000);
            }
        }
        return blockedExecutor;
    }

    public void addJobFlowNode(JobFlowNode jobFlowNode){
        if(this.jobFlowNodes == null){
            jobFlowNodes = new ArrayList<>();
        }
        jobFlowNode.setCompositionJobFlowNode(this);
        jobFlowNode.setContainerParrelJobFlowNodeContext(this.parrelJobFlowNodeContext);
        this.jobFlowNodes.add(jobFlowNode);
    }
    /**
     * 作业工作流每次调度执行并行分支节点时，重置并行分支节点执行状态
     */
    public void reset(){
        for (int i = 0; jobFlowNodes != null && i < jobFlowNodes.size(); i++) {
            JobFlowNode jobFlowNode = jobFlowNodes.get(i);
            jobFlowNode.reset();
        }
        super.reset();
        
    }
    /**
     * 启动流程当前节点
     */
    @Override
    public boolean start(CyclicBarrier barrier){
        parrelJobFlowNodeContext.updateJobFlowNodeStatus(JobFlowNodeStatus.STARTED);
        nodeStart();
        if(barrier != null) {
            try {
                barrier.await();
            } catch (InterruptedException e) {                
            } catch (BrokenBarrierException e) {
            }
        }
        jobFlow.getJobFlowContext().pauseAwait(this);
        JobFlowContext jobFlowContext = this.jobFlow.getJobFlowContext();
        AssertResult assertResult = jobFlowContext.assertStopped();
        if(assertResult.isTrue())
        {
            logger.info("AssertStopped: true,ignore execute {}.",this.getJobFlowNodeInfo());
//            nodeComplete(null,true);
            return false;
        }
        else if(assertTrigger()) {
            if (jobFlowNodes == null || jobFlowNodes.size() == 0) {
                throw new JobFlowException(this.getJobFlowNodeInfo()+" must set jobFlowNodes,please set jobFlowNodes first.");
            } else {
                jobFlowNodeExecuteContext = new DefaultJobFlowNodeExecuteContext(this);
                
                logger.info("Start {} begin.",this.getJobFlowNodeInfo());
                if(CollectionUtils.isNotEmpty(this.jobFlowNodeListeners)){
                    for(JobFlowNodeListener jobFlowNodeListener:jobFlowNodeListeners){
                        jobFlowNodeListener.beforeExecute(jobFlowNodeExecuteContext);
                    }
                }
                ExecutorService blockedExecutor = buildThreadPool();
                List<Future> futureList = new ArrayList<>();
                CyclicBarrier thisBarrier = new CyclicBarrier(jobFlowNodes.size(), () -> {
                    logger.info("All Parrel jobFlowNodes[{}] of {} ready to running.",jobFlowNodes.size(),this.getJobFlowNodeInfo());
                });
                for (int i = 0; i < jobFlowNodes.size(); i++) {
                    JobFlowNode jobFlowNode = jobFlowNodes.get(i);
                    futureList.add(blockedExecutor.submit(() -> {
                        jobFlowNode.start(thisBarrier);
                    }));
                    
                   
                }
                List<Throwable> exceptions = null;
                for(Future future:futureList){
                    try {
                        future.get();
                    } catch (InterruptedException e) {
//                        throw new RuntimeException(e);
                    } catch (ExecutionException e) {
                        if(exceptions == null){
                            exceptions = new ArrayList<>();
                        }
                        exceptions.add(e.getCause());
                    }
                }
                
//                if(CollectionUtils.isNotEmpty(exceptions)){
//                    logger.warn("Execute parrelJobFlowNode[id={},name={}] complete with exceptions.",this.getNodeId(),this.getNodeName());
//                    throw new JobFlowException("ParrelJobFlowNode execute exception:",exceptions);
//                }
//                else{
//                    logger.info("Execute parrelJobFlowNode[id={},name={}] complete.",this.getNodeId(),this.getNodeName());
//                }
//                nodeComplete(null);
            }
            return true;
        }
        else{
            logger.info("AssertTrigger: false,ignore execute {}.",this.getJobFlowNodeInfo());
            jobFlowNodeExecuteContext = new DefaultJobFlowNodeExecuteContext(this);
            if(CollectionUtils.isNotEmpty(this.jobFlowNodeListeners)){
                for(JobFlowNodeListener jobFlowNodeListener:jobFlowNodeListeners){
                    jobFlowNodeListener.beforeExecute(jobFlowNodeExecuteContext);
                }
            }
            nodeComplete(null,true,jobFlowNodeExecuteContext);
        }
        return false;
        
    }

    @Override
    protected void release(){
        if(blockedExecutor != null){
            blockedExecutor.shutdown();
        }
        super.release();
    }
    
    /**
     * 停止流程当前节点
     */
    @Override
    public void stop() {
        if(parrelJobFlowNodeContext.assertStoped()){
            return;
        }
        logger.info("Stop {} begin.",this.getJobFlowNodeInfo());
        parrelJobFlowNodeContext.updateJobFlowNodeStatus(JobFlowNodeStatus.STOPPING);
        for (int i = 0; jobFlowNodes != null && i < jobFlowNodes.size(); i++) {
            JobFlowNode jobFlowNode = jobFlowNodes.get(i);
            jobFlowNode.stop();
        }
        release();
        parrelJobFlowNodeContext.updateJobFlowNodeStatus(JobFlowNodeStatus.STOPED);
        logger.info("Stop {} complete.",this.getJobFlowNodeInfo());
        if(CollectionUtils.isNotEmpty(this.jobFlowNodeListeners)){
            for(JobFlowNodeListener jobFlowNodeListener:jobFlowNodeListeners){
                jobFlowNodeListener.afterEnd(this);
            }
        }
        if(this.nextJobFlowNode != null){
            this.nextJobFlowNode.stop();
        }
//        if(this.nextJobFlowNode != null){
//            try {
//                this.nextJobFlowNode.stop();
////                logger.warn("Stop nextJobFlowNode[id={},nodeName={}] complete.",this.getNodeId(),this.getNodeName());
//            }
//            catch (Exception e){
//                logger.warn("Stop nextJobFlowNode[id="+nextJobFlowNode.getNodeId()+",nodeName="+nextJobFlowNode.getNodeName()+"] failed:",e);
//            }
//        }
    }

    private Object jobFlowNodeExecuteContextLock = new Object();
    /**
     * 某个并行分支完成时回调
     * @param jobFlowNode
     */
    @Override
    public void brachComplete(JobFlowNode jobFlowNode, Throwable e) {
        if(this.parrelJobFlowNodeContext.allNodeComplete() ) {
            JobFlowNodeExecuteContext jobFlowNodeExecuteContext_ = null;
            synchronized (jobFlowNodeExecuteContextLock){
                jobFlowNodeExecuteContext_ = this.jobFlowNodeExecuteContext;
                this.jobFlowNodeExecuteContext = null;
            }
            if(jobFlowNodeExecuteContext_ != null) {
                this.nodeComplete(e, false,jobFlowNodeExecuteContext_);
            }
        }
    }

 

 
    /**
     * 暂停流程节点
     */
    @Override
    public void pause() {
        for (int i = 0; jobFlowNodes != null && i < jobFlowNodes.size(); i++) {
            JobFlowNode jobFlowNode = jobFlowNodes.get(i);
            jobFlowNode.pause();
        }
        parrelJobFlowNodeContext.updateJobFlowNodeStatus(JobFlowNodeStatus.PAUSE);
    }

    /**
     * 唤醒暂停流程节点
     */
    @Override
    public void consume() {
        parrelJobFlowNodeContext.updateJobFlowNodeStatus(JobFlowNodeStatus.RUNNING);
        for (int i = 0; jobFlowNodes != null && i < jobFlowNodes.size(); i++) {
            JobFlowNode jobFlowNode = jobFlowNodes.get(i);
            jobFlowNode.consume();
        }
    }
}
