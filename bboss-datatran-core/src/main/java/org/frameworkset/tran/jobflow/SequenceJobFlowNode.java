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
import org.frameworkset.tran.jobflow.context.AssertResult;
import org.frameworkset.tran.jobflow.context.JobFlowContext;
import org.frameworkset.tran.jobflow.context.SequenceJobFlowNodeContext;
import org.frameworkset.tran.schedule.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

/**
 * 顺序执行的复合流程节点
 * @author biaoping.yin
 * @Date 2025/3/31
 */
public class SequenceJobFlowNode extends CompositionJobFlowNode{
    private static Logger logger = LoggerFactory.getLogger(SequenceJobFlowNode.class);

    private JobFlowNode headerJobFlowNode;    
    
    private SequenceJobFlowNodeContext sequenceJobFlowNodeContext;
    public SequenceJobFlowNode(){
        this.jobFlowNodeType = JobFlowNodeType.SEQUENCE;
        
        sequenceJobFlowNodeContext = new SequenceJobFlowNodeContext(this);
        this.jobFlowNodeContext = sequenceJobFlowNodeContext;
    }

    public SequenceJobFlowNodeContext getSequenceJobFlowNodeContext() {
        return sequenceJobFlowNodeContext;
    }

    public void setHeaderJobFlowNode(JobFlowNode headerJobFlowNode) {
        this.headerJobFlowNode = headerJobFlowNode;
        headerJobFlowNode.setCompositionJobFlowNode(this);
        headerJobFlowNode.setContainerSequenceJobFlowNodeContext(this.sequenceJobFlowNodeContext);
    }
    /**
     * 作业工作流每次调度执行串行分支节点时，重置串行分支节点执行状态
     */
    public void reset(){
        this.headerJobFlowNode.reset();
        super.reset();
    }
    /**
     * 启动流程当前节点
     */
    @Override
    public boolean start(CyclicBarrier barrier){
        sequenceJobFlowNodeContext.updateJobFlowNodeStatus(JobFlowNodeStatus.STARTED);
        
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
            return false;
//            nodeComplete(null,true);
        }
        else if(assertTrigger()) {
            if (headerJobFlowNode == null) {
                throw new JobFlowException(this.getJobFlowNodeInfo()+":headerJobFlowNode is null.");
            } else {
                logger.info("Start {} begin.",this.getJobFlowNodeInfo());
//                for (int i = 0; i < jobFlowNodes.size(); i++) {
//                    JobFlowNode jobFlowNode = jobFlowNodes.get(i);
//                    if(jobFlowNode.start())
//                        startNodes.increament();
//                }                
                headerJobFlowNode.start();
//                logger.info("Execute SequenceJobFlowNode[id={},name={}] complete.",this.getNodeId(),this.getNodeName());
//                this.nodeComplete( null);
            }
            return true;
        }
        else{
            logger.info("AssertTrigger: false,ignore execute {}.",this.getJobFlowNodeInfo());
            nodeComplete(null,true);
        }
        return false;
        
    }

    /**
     * 停止流程当前节点
     */
    @Override
    public void stop() {
        if(sequenceJobFlowNodeContext.assertStoped()){
            return;
        }        
        logger.info("Stop {} begin.",this.getJobFlowNodeInfo());
        sequenceJobFlowNodeContext.updateJobFlowNodeStatus(JobFlowNodeStatus.STOPPING);
        try {
//            for (int i = 0; jobFlowNodes != null && i < jobFlowNodes.size(); i++) {
//                JobFlowNode jobFlowNode = jobFlowNodes.get(i);
//                jobFlowNode.stop();
//            }
            this.sequenceJobFlowNodeContext.stop();
            if(headerJobFlowNode != null) {
                this.headerJobFlowNode.stop();
            }
        }
        catch (Exception e){
            logger.warn("Stop "+this.getJobFlowNodeInfo()+" failed:",e);
        }
        sequenceJobFlowNodeContext.updateJobFlowNodeStatus(JobFlowNodeStatus.STOPED);
        logger.info("Stop {} complete.",this.getJobFlowNodeInfo());
        if(this.nextJobFlowNode != null){
            this.nextJobFlowNode.stop();
        }
//        if(this.nextJobFlowNode != null){
//            try {
//                this.nextJobFlowNode.stop();
////                logger.warn("Stop nextJobFlowNode of["+this.getNodeName()+"] complete.");
//            }
//            catch (Exception e){
//                logger.warn("Stop nextJobFlowNode[id="+nextJobFlowNode.getNodeId()+",name="+nextJobFlowNode.getNodeName()+"] failed:",e);
//            }
//        }
    }

    /**
     * 串行分支全部完成是回调
     * @param jobFlowNode
     */
    @Override
    public void brachComplete(JobFlowNode jobFlowNode, ImportContext importContext, Throwable e) {
//        if(liveNodes <= 0){
//            this.nodeComplete(  importContext,   e);
//        }
        this.nodeComplete(importContext,e);
    }

    /**
     * 串行分支全部完成是回调
     *
     * @param jobFlowNode
     * @param e
     */
    @Override
    public void brachComplete(JobFlowNode jobFlowNode, Throwable e) {
        this.nodeComplete(e,false);
    }

    /**
     * 串行分支全部完成是回调
     *
     * @param jobFlowNode
     * @param taskContext
     * @param e
     */
    @Override
    public void brachComplete(JobFlowNode jobFlowNode, TaskContext taskContext, Throwable e) {
        this.nodeComplete(e,false);
    }

    /**
     * 暂停流程节点
     */
    @Override
    public void pause() {
        sequenceJobFlowNodeContext.pause();
        sequenceJobFlowNodeContext.updateJobFlowNodeStatus(JobFlowNodeStatus.PAUSE);
//        for (int i = 0; jobFlowNodes != null && i < jobFlowNodes.size(); i++) {
//            JobFlowNode jobFlowNode = jobFlowNodes.get(i);
//            jobFlowNode.pause();
//        }
    }

    /**
     * 唤醒暂停流程节点
     */
    @Override
    public void consume() {
//        for (int i = 0; jobFlowNodes != null && i < jobFlowNodes.size(); i++) {
//            JobFlowNode jobFlowNode = jobFlowNodes.get(i);
//            jobFlowNode.consume();
//        }
        sequenceJobFlowNodeContext.updateJobFlowNodeStatus(JobFlowNodeStatus.RUNNING);
        sequenceJobFlowNodeContext.consume();
    }
}
