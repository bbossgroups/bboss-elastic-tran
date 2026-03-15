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
    private ExecutorService buildThreadPool(){
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



    /**
     * 获取线程池监控信息
     * @return 包含线程池各项指标的字符串
     */
    public String getThreadPoolMetrics() {
        if (blockedExecutor instanceof ThreadPoolExecutor) {
            ThreadPoolExecutor executor = (ThreadPoolExecutor) blockedExecutor;
            return String.format(
                    "ParrelJobFlowNode[%s] ThreadPoolMetrics: active=%d, poolSize=%d, corePoolSize=%d, maxPoolSize=%d, queueSize=%d, completed=%d",
                    this.nodeName,
                    executor.getActiveCount(),
                    executor.getPoolSize(),
                    executor.getCorePoolSize(),
                    executor.getMaximumPoolSize(),
                    executor.getQueue().size(),
                    executor.getCompletedTaskCount()
            );
        }
        return "Thread pool not initialized";
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
    public boolean execute(JobFlowNodeExecuteContext jobFlowNodeExecuteContext,JobFlowCyclicBarrier barrier){
        try {
            logger.info("Execute {} begin.", this.getJobFlowNodeInfo());
            lastException = null;
            this.jobFlowNodeExecuteContext = jobFlowNodeExecuteContext;
            jobFlowNodeExecuteContext.updateJobFlowNodeStatus(JobFlowNodeStatus.STARTED);
            nodeStart();
            if (barrier != null) {
                try {
                    logger.info("Execute {} barrier.await().", this.getJobFlowNodeInfo());
                    barrier.await();
                } catch (InterruptedException e) {
                } catch (BrokenBarrierException e) {
                } catch (TimeoutException e) {
                }
            }
            jobFlow.getJobFlowContext().pauseAwait(this);
            JobFlowContext jobFlowContext = this.jobFlow.getJobFlowContext();
            AssertResult assertResult = jobFlowContext.assertStopped();
            if (assertResult.isTrue()) {
                logger.info("AssertStopped: true,ignore execute {}.", this.getJobFlowNodeInfo());
//            nodeComplete(null,true);
                return false;
            } else if (assertTrigger()) {
                if (jobFlowNodes == null || jobFlowNodes.size() == 0) {
                    logger.info("Execute {} jobFlowNodes == null || jobFlowNodes.size() == 0, must set jobFlowNodes,please set jobFlowNodes first. .", this.getJobFlowNodeInfo());
                    throw new JobFlowException(this.getJobFlowNodeInfo() + " must set jobFlowNodes,please set jobFlowNodes first.");
                } else {
//                jobFlowNodeExecuteContext = new DefaultJobFlowNodeExecuteContext(this);
                    try {
                        logger.info("Start {} begin.", this.getJobFlowNodeInfo());
                        if (CollectionUtils.isNotEmpty(this.jobFlowNodeListeners)) {
                            for (JobFlowNodeListener jobFlowNodeListener : jobFlowNodeListeners) {

                                try {
                                    jobFlowNodeListener.beforeExecute(jobFlowNodeExecuteContext);
                                } catch (Exception e) {
                                    logger.warn(this.getJobFlowNodeInfo() + "JobFlowNodeListener.beforeExecute failed:", e);
//                            throw new JobFlowException(this.getJobFlowNodeInfo()+" JobFlowNodeListener.beforeExecute failed:",e);
                                }
                            }
                        }
                        ExecutorService blockedExecutor = buildThreadPool();
//                logger.info("{} {}",this.getJobFlowNodeInfo(),getThreadPoolMetrics());
                        List<Future> futureList = new ArrayList<>();
                        JobFlowCyclicBarrier thisBarrier = new JobFlowCyclicBarrier(jobFlowNodes.size(), () -> {
                            logger.info("{} Parrel jobFlowNodes of {} ready to running.", jobFlowNodes.size(), this.getJobFlowNodeInfo());
                        }, 1000000L);
                        for (int i = 0; i < jobFlowNodes.size(); i++) {
                            JobFlowNode jobFlowNode = jobFlowNodes.get(i);
                            futureList.add(blockedExecutor.submit(() -> {
                                JobFlowNodeExecuteContext _jobFlowNodeExecuteContext = jobFlowNode.buildJobFlowNodeExecuteContext();
                                //todo call assertTrigger 
                                _jobFlowNodeExecuteContext.setContainerParrelJobFlowNodeExecuteContext(jobFlowNodeExecuteContext);
                                jobFlowNode.execute(_jobFlowNodeExecuteContext, thisBarrier);
                            }));


                        }
                        List<Throwable> exceptions = null;
                        for (Future future : futureList) {
                            try {
                                future.get();
                            } catch (InterruptedException e) {
//                        throw new RuntimeException(e);
                            } catch (ExecutionException e) {
                                if (exceptions == null) {
                                    exceptions = new ArrayList<>();
                                }
                                exceptions.add(e.getCause());
                            }
                        }

                    } finally {
                        this.nodeComplete(lastException, false);
                    }


                }
                return true;
            } else {
                logger.info("AssertTrigger: false,ignore execute {}.", this.getJobFlowNodeInfo());
//            jobFlowNodeExecuteContext = buildJobFlowNodeExecuteContext();
                if (CollectionUtils.isNotEmpty(this.jobFlowNodeListeners)) {
                    for (JobFlowNodeListener jobFlowNodeListener : jobFlowNodeListeners) {

                        try {
                            jobFlowNodeListener.beforeExecute(jobFlowNodeExecuteContext);
                        } catch (Exception e) {
                            logger.warn(this.getJobFlowNodeInfo() + "JobFlowNodeListener.beforeExecute failed:", e);
//                        throw new JobFlowException(this.getJobFlowNodeInfo()+" JobFlowNodeListener.beforeExecute failed:",e);
                        }
                    }
                }
                nodeComplete(null, true);
            }
            return false;
        }
        catch (Exception e){
            logger.error(this.getJobFlowNodeInfo()+" execute failed:",e);
            lastException = e;
            return false;
        }
        
    }

 
    
    /**
     * 停止流程当前节点
     */
    @Override
    public void stop() {
        //节点未执行，无需stop
        if(jobFlowNodeExecuteContext == null){
            return;
        }

        if(jobFlowNodeExecuteContext.assertStoped()){
            return;
        }
        logger.info("Stop {} begin.",this.getJobFlowNodeInfo());
        jobFlowNodeExecuteContext.updateJobFlowNodeStatus(JobFlowNodeStatus.STOPPING);
        for (int i = 0; jobFlowNodes != null && i < jobFlowNodes.size(); i++) {
            JobFlowNode jobFlowNode = jobFlowNodes.get(i);
            jobFlowNode.stop();
        }
        if(blockedExecutor != null){
            blockedExecutor.shutdown();
        }
        release();
        jobFlowNodeExecuteContext.updateJobFlowNodeStatus(JobFlowNodeStatus.STOPED);
        logger.info("Stop {} complete.",this.getJobFlowNodeInfo());
        if(CollectionUtils.isNotEmpty(this.jobFlowNodeListeners)){
            for(JobFlowNodeListener jobFlowNodeListener:jobFlowNodeListeners){
                
                try {
                    jobFlowNodeListener.afterEnd(this);
                }
                catch (Exception e){
                    logger.warn(this.getJobFlowNodeInfo()+"JobFlowNodeListener.afterEnd failed:",e);
//                    throw new JobFlowException(this.getJobFlowNodeInfo()+" obFlowNodeListener.afterEnd failed:",e);
                }
            }
        }
        if(this.nextJobFlowNode != null){
            this.nextJobFlowNode.stop();
        }

    }

    private Object jobFlowNodeExecuteContextLock = new Object();
    private Throwable lastException;
    /**
     * 某个并行分支完成时回调
     * @param jobFlowNode
     */
    @Override
    public void brachComplete(JobFlowNode jobFlowNode, Throwable e) {
        if(e != null)
            lastException = e;
//        if(this.jobFlowNodeExecuteContext.allNodeComplete() ) {
//             this.nodeComplete(e, false);
//        }
    }

 

 
    /**
     * 暂停流程节点
     */
    @Override
    public void pause() {
        //节点未执行，无需暂停
        if(jobFlowNodeExecuteContext == null){
            return;
        }

        for (int i = 0; jobFlowNodes != null && i < jobFlowNodes.size(); i++) {
            JobFlowNode jobFlowNode = jobFlowNodes.get(i);
            jobFlowNode.pause();
        }
        jobFlowNodeExecuteContext.updateJobFlowNodeStatus(JobFlowNodeStatus.PAUSE);
    }

    /**
     * 唤醒暂停流程节点
     */
    @Override
    public void consume() {
        //节点未执行，无需唤醒
        if(jobFlowNodeExecuteContext == null){
            return;
        }

        jobFlowNodeExecuteContext.updateJobFlowNodeStatus(JobFlowNodeStatus.RUNNING);
        for (int i = 0; jobFlowNodes != null && i < jobFlowNodes.size(); i++) {
            JobFlowNode jobFlowNode = jobFlowNodes.get(i);
            jobFlowNode.consume();
        }
    }
}
