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

import com.frameworkset.util.SimpleStringUtil;
import org.frameworkset.tran.jobflow.context.ConditionJobFlowNodeExecuteContext;
import org.frameworkset.tran.jobflow.context.DefaultJobFlowNodeExecuteContext;
import org.frameworkset.tran.jobflow.context.JobFlowExecuteContext;
import org.frameworkset.tran.jobflow.context.JobFlowNodeExecuteContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.TimeoutException;

/**
 *
 * @author biaoping.yin
 * @Date 2025/3/31
 */
public class ConditionJobFlowNode extends CompositionJobFlowNode{
    private static Logger logger = LoggerFactory.getLogger(ConditionJobFlowNode.class);
    private String conditionJobFlowNodeUUID;


    
    /**
     * 默认节点
     */
    private JobFlowNode defaultJobFlowNode;

    /**
     * 匹配的节点
     */
    private JobFlowNode matchedJobFlowNode;
    public ConditionJobFlowNode(){
        super();
        this.conditionJobFlowNodeUUID = SimpleStringUtil.getUUID32();
    }

    public ConditionJobFlowNode(String conditionJobFlowNodeUUID){
        super();
        this.conditionJobFlowNodeUUID = conditionJobFlowNodeUUID;
    }

    public String getConditionJobFlowNodeUUID() {
        return conditionJobFlowNodeUUID;
    }

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

     
    public ConditionJobFlowNode addJobFlowNode(JobFlowNode jobFlowNode,NodeTrigger conditionNodeTrigger){        
        if(jobFlowNodes == null)
            jobFlowNodes = new ArrayList<>();
        jobFlowNodes.add(jobFlowNode);
        if(conditionNodeTrigger != null){
            jobFlowNode.addConditionNodeTrigger(this.getConditionJobFlowNodeUUID(),conditionNodeTrigger);
        }
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
            if(jobFlowNode.getNodeTrigger() == null || jobFlowNode.getConditionNodeTrigger(this.conditionJobFlowNodeUUID) == null){
                logger.warn("流程{}中的条件流程节点{}没有配置条件判断器,忽略。",this.getJobFlow().getJobInfo(),jobFlowNode.getJobFlowNodeInfo()   );
                continue;
            }
            if(jobFlowNode.assertTrigger(conditionJobFlowNodeUUID)){
                matchedJobFlowNode = jobFlowNode;
                break;
            }
        }
        if(matchedJobFlowNode == null) {
            if (defaultJobFlowNode != null) {
                matchedJobFlowNode = defaultJobFlowNode;
            } else {
                return null;
            }
        }
        return matchedJobFlowNode;
    }


    /**
     * 当条件分支如果没有匹配到任何条件节点，也没有默认节点，则直接跳过条件节点，是否终止条件节点后续节点执行呢？处理逻辑如下：
     * 判断条件分支是不是在工作流容器的调度周期或者所属容器的调度周期内第一次执行
     * 如果是第一次，则返回true并终止条件分支后续的流程节点执行，否则返回false，然后执行条件分支后续的流程节点
     * @return
     */
    private boolean checkFirstExecuteInContainerLifeCycle(JobFlowNodeExecuteContext jobFlowNodeExecuteContext){
        JobFlowNodeExecuteContext containerJobFlowNodeExecuteContext = jobFlowNodeExecuteContext.getContainerJobFlowNodeExecuteContext();
        if(containerJobFlowNodeExecuteContext != null){
            return containerJobFlowNodeExecuteContext.checkFirstExecuteInContainerLifeCycle(this);
        }
        JobFlowExecuteContext containerJobFlowExecuteContext = jobFlowNodeExecuteContext.getContainerJobFlowExecuteContext();
        if(containerJobFlowExecuteContext != null){
            return containerJobFlowExecuteContext.checkFirstExecuteInContainerLifeCycle(this);
        }
        //既不在容器调度周期内，也不在工作流调度周期内，这种情况不可能存在，但是还是返回true，不执行条件分支后续的流程节点
        return true;
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
            if(matchedJobFlowNode == null) {
                boolean checkFirstExecuteInContainerLifeCycle = checkFirstExecuteInContainerLifeCycle(jobFlowNodeExecuteContext);
                if(checkFirstExecuteInContainerLifeCycle){
                    logger.info("流程{}中的条件流程节点{}没有匹配到任何条件，且没有默认节点，终止条件节点后续节点执行。",this.getJobFlow().getJobInfo(),this.getJobFlowNodeInfo()   );
                }
                nodeComplete(null,checkFirstExecuteInContainerLifeCycle);
                return false;
            }
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
        if(matchedJobFlowNode != null)
            matchedJobFlowNode.stop();
    }

    /**
     * 暂停流程节点
     */
    @Override
    public void pause() {
        if(matchedJobFlowNode != null)
            matchedJobFlowNode.pause();

    }

    /**
     * 唤醒暂停流程节点
     */
    @Override
    public void consume() {
        if(matchedJobFlowNode != null)
            matchedJobFlowNode.consume();
    }
}
