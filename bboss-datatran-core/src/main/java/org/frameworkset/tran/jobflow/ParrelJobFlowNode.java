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
import org.frameworkset.util.concurrent.IntegerCount;
import org.frameworkset.util.concurrent.ThreadPoolFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * 并行任务流程节点
 * @author biaoping.yin
 * @Date 2025/3/31
 */
public class ParrelJobFlowNode extends CompositionJobFlowNode{
    private static Logger logger = LoggerFactory.getLogger(ParrelJobFlowNode.class);
    private ExecutorService blockedExecutor;
    private Object blockedExecutorLock = new Object();
    public ParrelJobFlowNode(){
        this.jobFlowNodeType = JobFlowNodeType.PARREL;
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
   

    /**
     * 启动流程当前节点
     */
    @Override
    public boolean start(){
        
        startNodes = new IntegerCount();
        if(assertTrigger()) {
            if (jobFlowNodes == null || jobFlowNodes.size() == 0) {
                throw new JobFlowException("ParrelJobFlowNode must set jobFlowNodes,please set jobFlowNodes first.");
            } else {

                logger.info("Start parrelJobFlowNode[id={},name={}] begin.",this.getNodeId(),this.getNodeName());
                ExecutorService blockedExecutor = buildThreadPool();
                List<Future> futureList = new ArrayList<>();
                for (int i = 0; i < jobFlowNodes.size(); i++) {
                    JobFlowNode jobFlowNode = jobFlowNodes.get(i);
                    futureList.add(blockedExecutor.submit(() -> {
                        if(jobFlowNode.start())
                            startNodes.increament();
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
                
                if(CollectionUtils.isNotEmpty(exceptions)){
                    logger.warn("Execute parrelJobFlowNode[id={},name={}] complete with exceptions.",this.getNodeId(),this.getNodeName());
                    throw new JobFlowException("ParrelJobFlowNode execute exception:",exceptions);
                }
                else{
                    logger.info("Execute parrelJobFlowNode[id={},name={}] complete.",this.getNodeId(),this.getNodeName());
                }
                nodeComplete(null);
            }
            return true;
        }
        else{
            logger.info("AssertTrigger: false,ignore execute this ParrelJobFlowNode.");
            nodeComplete(null);
        }
        return false;
        
    }

    /**
     * 停止流程当前节点
     */
    /**
     * 停止流程当前节点
     */
    @Override
    public void stop() {
         
        for (int i = 0; jobFlowNodes != null && i < jobFlowNodes.size(); i++) {
            JobFlowNode jobFlowNode = jobFlowNodes.get(i);
            jobFlowNode.stop();
        }
        if(blockedExecutor != null){
            blockedExecutor.shutdown();
        }

        logger.info("Stop ParrelJobFlowNode[id={},nodeName={}] complete.",this.getNodeId(),this.getNodeName());
        if(this.nextJobFlowNode != null){
            try {
                this.nextJobFlowNode.stop();
//                logger.warn("Stop nextJobFlowNode[id={},nodeName={}] complete.",this.getNodeId(),this.getNodeName());
            }
            catch (Exception e){
                logger.warn("Stop nextJobFlowNode[id="+nextJobFlowNode.getNodeId()+",nodeName="+nextJobFlowNode.getNodeName()+"] failed:",e);
            }
        }
    }

    
}
