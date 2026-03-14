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

import org.frameworkset.tran.jobflow.context.ConditionJobFlowNodeExecuteContext;
import org.frameworkset.tran.jobflow.context.DefaultJobFlowNodeExecuteContext;
import org.frameworkset.tran.jobflow.context.JobFlowNodeExecuteContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.TimeoutException;

/**
 *
 * @author biaoping.yin
 * @Date 2025/3/31
 */
public class ConditionJobFlowNode extends CompositionJobFlowNode{
    private static Logger logger = LoggerFactory.getLogger(ConditionJobFlowNode.class);
    /**
     * 默认节点
     */
    private JobFlowNode defaultJobFlowNode;

    /**
     * 匹配的节点
     */
    private JobFlowNode matchedJobFlowNode;

    public JobFlowNodeExecuteContext buildJobFlowNodeExecuteContext( ) {
        return new ConditionJobFlowNodeExecuteContext(this);
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
     

    public void setDefaultJobFlowNode(JobFlowNode defaultJobFlowNode) {
        this.defaultJobFlowNode = defaultJobFlowNode;
    }

     
    public ConditionJobFlowNode addJobFlowNode(JobFlowNode jobFlowNode){        
        if(jobFlowNodes == null)
            jobFlowNodes = new ArrayList<JobFlowNode>();
        jobFlowNodes.add(jobFlowNode);
        
        jobFlowNode.addConditionJobFlowNode(this);
        return this;
    }

    /**
     * 查找匹配条件的流程执行节点
     * @return
     */
    public JobFlowNode evalJobFlowNode() {
        matchedJobFlowNode = null;
        for(JobFlowNode jobFlowNode : jobFlowNodes){
            if(jobFlowNode.getNodeTrigger() == null){
                logger.warn("流程{}中的条件流程节点{}没有配置条件判断器,忽略。",this.getJobFlow().getJobInfo(),jobFlowNode.getJobFlowNodeInfo()   );
                continue;
            }
            if(jobFlowNode.assertTrigger()){
                matchedJobFlowNode = jobFlowNode;
                break;
            }
        }
        if(matchedJobFlowNode == null) {
            if (defaultJobFlowNode != null) {
                matchedJobFlowNode = defaultJobFlowNode;
            } else {
                throw new JobFlowException("没有符合条件的节点，也没有配置默认的节点.");
            }
        }
        return matchedJobFlowNode;
    }


    /**
     * 启动流程当前节点
     * @param jobFlowNodeExecuteContext
     * @param barrier
     */
    @Override
    public boolean execute(JobFlowNodeExecuteContext jobFlowNodeExecuteContext,JobFlowCyclicBarrier barrier) {
        if(logger.isDebugEnabled()) {
            logger.debug("Execute [{}] entered", getJobFlowNodeInfo());
        }
        if(barrier != null) {
            try {
                barrier.await();
            } catch (InterruptedException e) {
            } catch (BrokenBarrierException e) {
            } catch (TimeoutException e) {
            }
        }
        if(logger.isDebugEnabled()){
            logger.debug("Execute [{}] begin", getJobFlowNodeInfo());
        }
        try {

            this.jobFlowNodeExecuteContext = jobFlowNodeExecuteContext;
            evalJobFlowNode();
            JobFlowNodeExecuteContext matchedJobFlowNodeExecuteContext = new DefaultJobFlowNodeExecuteContext(matchedJobFlowNode);
            matchedJobFlowNodeExecuteContext.setContainerConditionJobFlowNodeExecuteContext(jobFlowNodeExecuteContext);
            ((ConditionJobFlowNodeExecuteContext)jobFlowNodeExecuteContext).setMatchedJobFlowNodeExecuteContext(matchedJobFlowNodeExecuteContext);
            boolean result = matchedJobFlowNode.execute(matchedJobFlowNodeExecuteContext, barrier);
            if(logger.isDebugEnabled()){
                logger.debug("Execute [{}] complete", getJobFlowNodeInfo());
            }
            return result;
        }
        catch (JobFlowException e)
        {
            logger.error("",e);
            nodeComplete(e,true);
            return false;
        }

        catch (Exception e)
        {
            logger.error("",e);
            nodeComplete(e,true);
            return false;
            
        }
       
         
    }

    /**
     * 停止流程当前节点
     */
    @Override
    public void stop() {
        matchedJobFlowNode.stop();
    }

    /**
     * 暂停流程节点
     */
    @Override
    public void pause() {
        matchedJobFlowNode.pause();

    }

    /**
     * 唤醒暂停流程节点
     */
    @Override
    public void consume() {
        matchedJobFlowNode.consume();
    }
}
