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

import org.frameworkset.tran.DataStream;
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.jobflow.context.AssertResult;
import org.frameworkset.tran.jobflow.context.JobFlowContext;
import org.frameworkset.tran.jobflow.context.SimpleJobFlowNodeContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

/**
 *
 * @author biaoping.yin
 * @Date 2025/3/31
 */
public class SimpleJobFlowNode extends JobFlowNode{
    private static Logger logger = LoggerFactory.getLogger(SimpleJobFlowNode.class);
 
    /**
     * 串行节点作业配置
     */
    private ImportBuilder importBuilder;
    private SimpleJobFlowNodeContext simpleJobFlowNodeContext;
 

    private DataStream dataStream;
    public SimpleJobFlowNode(ImportBuilder importBuilder, NodeTrigger nodeTrigger ){
        this.importBuilder = importBuilder;
        if(importBuilder == null){
            throw new JobFlowException("ImportBuilder is null.");
        }
        this.importBuilder.setJobFlowNode(this);
        this.nodeTrigger = nodeTrigger;
        simpleJobFlowNodeContext = new SimpleJobFlowNodeContext(this);
        this.jobFlowNodeContext = simpleJobFlowNodeContext;
    }

    public SimpleJobFlowNode(ImportBuilder importBuilder){
        this(importBuilder,null);
    }


    /**
     * 作业工作流每次调度执行串行分支节点时，重置串行分支节点执行状态
     */
    public void reset(){
        dataStream = null;
        super.reset();
    }
    /**
     * 启动流程当前节点
     */
    @Override
    public boolean start(CyclicBarrier barrier){
        simpleJobFlowNodeContext.updateJobFlowNodeStatus(JobFlowNodeStatus.STARTED);
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
        else if(this.assertTrigger()) {
            logger.info("Start {} begin.",this.getJobFlowNodeInfo());
            dataStream = importBuilder.builder(true);
            dataStream.execute();
            
            
//            this.nodeComplete(null);
            return true;
        }
        else{
            logger.info("AssertTrigger: false,ignore execute {}.",this.getJobFlowNodeInfo());
            nodeComplete(null,true);
        }
        return false;
    }
    protected void release(){
        if(dataStream != null){
            dataStream = null;
        }
    }
    /**
     * 停止流程当前节点
     */
    @Override
    public void stop(){
        if(simpleJobFlowNodeContext.assertStoped())
            return;
        logger.info("Stop {} begin.",this.getJobFlowNodeInfo());
        simpleJobFlowNodeContext.updateJobFlowNodeStatus(JobFlowNodeStatus.STOPPING);
        if(dataStream != null){
            dataStream.destroy(true);
        }
        
        simpleJobFlowNodeContext.updateJobFlowNodeStatus(JobFlowNodeStatus.STOPED);
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
     * 暂停流程节点
     */
    @Override
    public void pause(){
        
        if(dataStream != null){
            dataStream.pauseSchedule();
            simpleJobFlowNodeContext.updateJobFlowNodeStatus(JobFlowNodeStatus.PAUSE);
        }
    }

    /**
     * 唤醒暂停流程节点
     */
    @Override
    public void consume() {
        simpleJobFlowNodeContext.updateJobFlowNodeStatus(JobFlowNodeStatus.RUNNING);
        if(dataStream != null){
            dataStream.resumeSchedule();
        }
    }

 

    
    
}
