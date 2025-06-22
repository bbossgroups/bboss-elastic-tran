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
import org.frameworkset.tran.jobflow.context.AssertResult;
import org.frameworkset.tran.jobflow.context.DefaultJobFlowNodeExecuteContext;
import org.frameworkset.tran.jobflow.context.JobFlowContext;
import org.frameworkset.tran.jobflow.context.SimpleJobFlowNodeContext;
import org.frameworkset.tran.jobflow.listener.JobFlowNodeListener;
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
//    private ImportBuilder importBuilder;
    private SimpleJobFlowNodeContext simpleJobFlowNodeContext;
 
    private JobFlowNodeFunction jobFlowNodeFunction;
//    private DataStream dataStream;
    public SimpleJobFlowNode(JobFlowNodeFunction jobFlowNodeFunction, NodeTrigger nodeTrigger ){
        this.jobFlowNodeFunction = jobFlowNodeFunction;
        if(jobFlowNodeFunction == null){
            throw new JobFlowException("jobFlowNodeFunction is null.");
        }
        jobFlowNodeFunction.init(this);
//        this.importBuilder.setJobFlowNode(this);
        this.nodeTrigger = nodeTrigger;
        simpleJobFlowNodeContext = new SimpleJobFlowNodeContext(this);
        this.jobFlowNodeContext = simpleJobFlowNodeContext;
    }

    public SimpleJobFlowNode(JobFlowNodeFunction jobFlowNodeFunction){
        this(jobFlowNodeFunction,null);
    }


    /**
     * 作业工作流每次调度执行串行分支节点时，重置串行分支节点执行状态
     */
    public void reset(){
//        dataStream = null;
        jobFlowNodeFunction.reset();
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
            jobFlowNodeExecuteContext = new DefaultJobFlowNodeExecuteContext(this.jobFlow.getJobFlowExecuteContext());
            if(CollectionUtils.isNotEmpty(this.jobFlowNodeListeners)){
                for(JobFlowNodeListener jobFlowNodeListener:jobFlowNodeListeners){
                    jobFlowNodeListener.beforeExecute(jobFlowNodeExecuteContext);
                }
            }
            logger.info("Start {} begin.",this.getJobFlowNodeInfo());
//            dataStream = importBuilder.builder(true);
//            dataStream.execute();
            jobFlowNodeFunction.call(jobFlowNodeExecuteContext);
            
            
//            this.nodeComplete(null);
            return true;
        }
        else{
            jobFlowNodeExecuteContext = new DefaultJobFlowNodeExecuteContext(this.jobFlow.getJobFlowExecuteContext());
            if(CollectionUtils.isNotEmpty(this.jobFlowNodeListeners)){
                for(JobFlowNodeListener jobFlowNodeListener:jobFlowNodeListeners){
                    jobFlowNodeListener.beforeExecute(jobFlowNodeExecuteContext);
                }
            }
            logger.info("AssertTrigger: false,ignore execute {}.",this.getJobFlowNodeInfo());
            nodeComplete(null,true,jobFlowNodeExecuteContext);
        }
        return false;
    }
    protected void release(){
//        if(dataStream != null){
//            dataStream = null;
//        }
        jobFlowNodeFunction.release();
        super.release();
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
//        if(dataStream != null){
//            dataStream.destroy(true);
//        }

        jobFlowNodeFunction.stop();
        
        simpleJobFlowNodeContext.updateJobFlowNodeStatus(JobFlowNodeStatus.STOPED);
        logger.info("Stop {} complete.",this.getJobFlowNodeInfo());
        if(CollectionUtils.isNotEmpty(this.jobFlowNodeListeners)){
            for(JobFlowNodeListener jobFlowNodeListener:jobFlowNodeListeners){
                jobFlowNodeListener.afterEnd(this);
            }
        }
        if(this.nextJobFlowNode != null){
            this.nextJobFlowNode.stop();
        }

    }

    /**
     * 暂停流程节点
     */
    @Override
    public void pause(){
        
//        if(dataStream != null){
//            dataStream.pauseSchedule();
//            simpleJobFlowNodeContext.updateJobFlowNodeStatus(JobFlowNodeStatus.PAUSE);
//        }
        if(jobFlowNodeFunction != null){
            jobFlowNodeFunction.pauseSchedule();
            simpleJobFlowNodeContext.updateJobFlowNodeStatus(JobFlowNodeStatus.PAUSE);
        }
    }

    /**
     * 唤醒暂停流程节点
     */
    @Override
    public void consume() {
        simpleJobFlowNodeContext.updateJobFlowNodeStatus(JobFlowNodeStatus.RUNNING);
//        if(dataStream != null){
//            dataStream.resumeSchedule();
//        }
        if(jobFlowNodeFunction != null){
            jobFlowNodeFunction.resumeSchedule();
        }
    }

 

    
    
}
