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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


/**
 *
 * @author biaoping.yin
 * @Date 2025/3/31
 */
public abstract class JobFlowNode {
    private static Logger logger_ = LoggerFactory.getLogger(JobFlowNode.class);
    protected JobFlowNodeType jobFlowNodeType = JobFlowNodeType.SIMPLE;
    protected String nodeId;
    protected String nodeName;
    protected JobFlow jobFlow;
    protected String jobFlowNodeInfo;
    /**
     * 节点所属的容器节点
     */
    protected CompositionJobFlowNode compositionJobFlowNode;
    /**
     * 条件流程节点所属的条件复合节点清单
     * 条件复合节点包含了条件流程节点的parent节点
     */
    protected List<ConditionJobFlowNode> conditionJobFlowNodes;
    protected ConditionJobFlowNode runningConditionJobFlowNode;
    /**
     * 流程节点可能出现在不同的条件复合节点中，可以为对应的不同的条件复合节点指定不同的触发器，以条件复合节点的id为key
     */
    protected Map<String,NodeTrigger> conditionNodeTriggers = new LinkedHashMap<>();
    public JobFlowNodeExecuteContext buildJobFlowNodeExecuteContext( ) {
        return new DefaultJobFlowNodeExecuteContext(this);
    }
    
    /**
     * 串行节点触发条件
     */
    protected NodeTrigger nodeTrigger;
    /**
     * 父节点作业配置
     * 父节点可能是串行节点，也可能是并行节点
     */
    protected JobFlowNode parentJobFlowNode;
    protected Stack<JobFlowNode> parentJobFlowNodeStack;
    protected List<JobFlowNodeListener> jobFlowNodeListeners;
    /**
     * 下一节点作业配置
     * 下一节点可能是串行节点，也可能是并行节点
     */
    protected JobFlowNode nextJobFlowNode;

    /**
     * 用于跟踪串行分支节点执行情况
     */
    protected SequenceJobFlowNodeContext containerSequenceJobFlowNodeContext;



    /**
     * 用于跟踪和记录并行分支节点执行情况
     */
    protected ParrelJobFlowNodeContext containerParrelJobFlowNodeContext;


    


    protected JobFlowNodeExecuteContext jobFlowNodeExecuteContext;
    /**
     * 跟踪和记录工作流主干节点执行情况
     */
    protected JobFlowContext containerJobFlowContext;
    
   
    
    protected JobFlowNodeContext jobFlowNodeContext;


    /**
     * release方法需要再流程完成后统一执行
     */
    protected void release(){
        //todo 在流程结束时调用
        if(jobFlowNodeExecuteContext != null) {
            jobFlowNodeExecuteContext .clear();
//            jobFlowNodeExecuteContext = null;
        }
    }

    public void setJobFlowNodeListeners(List<JobFlowNodeListener> jobFlowNodeListeners) {
        this.jobFlowNodeListeners = jobFlowNodeListeners;
    }

    public List<JobFlowNodeListener> getJobFlowNodeListeners() {
        return jobFlowNodeListeners;
    }
    /**
     * 获取子节点对应的复合节点执行上下文
     *
     * @return
     */
    public JobFlowNodeExecuteContext getContainerJobFlowNodeExecuteContext() {
//        if(compositionJobFlowNode != null) {
//            return compositionJobFlowNode.getJobFlowNodeExecuteContext();
//        }
//        else{
//            return null;
//        }
        return this.jobFlowNodeExecuteContext.getContainerJobFlowNodeExecuteContext();
    }
    public JobFlowNodeExecuteContext getJobFlowNodeExecuteContext() {
        return jobFlowNodeExecuteContext;
    }

    protected String buildJobFlowNodeInfo(){
        if(this.jobFlowNodeInfo == null) {
            StringBuilder info = new StringBuilder();
            info.append(this.getClass().getSimpleName())
                    .append("[id=").append(this.getNodeId())
                    .append(",nodeName=").append(this.getNodeName())
                    .append(",nodeType=").append(this.jobFlowNodeType.name())
                    .append("]");
            return jobFlowNodeInfo = info.toString();
        }
        return jobFlowNodeInfo;
    }
    public String getJobFlowNodeInfo() {
        return buildJobFlowNodeInfo();
    }

    public void setCompositionJobFlowNode(CompositionJobFlowNode compositionJobFlowNode) {
        this.compositionJobFlowNode = compositionJobFlowNode;
    }

    public JobFlowNodeContext getJobFlowNodeContext() {
        return jobFlowNodeContext;
    }

    public void setContainerSequenceJobFlowNodeContext(SequenceJobFlowNodeContext containerSequenceJobFlowNodeContext) {
        this.containerSequenceJobFlowNodeContext = containerSequenceJobFlowNodeContext;
        if(this.nextJobFlowNode != null){
            this.nextJobFlowNode.setContainerSequenceJobFlowNodeContext(containerSequenceJobFlowNodeContext);
        }
    }

//    public void setContainerSequenceJobFlowNodeExecuteContext(SequenceJobFlowNodeExecuteContext containerSequenceJobFlowNodeExecuteContext) {
//        this.containerSequenceJobFlowNodeExecuteContext = containerSequenceJobFlowNodeExecuteContext;
//        this.containerSequenceJobFlowNodeContext = (SequenceJobFlowNodeContext)containerSequenceJobFlowNodeExecuteContext.getJobFlowNodeContext();
//        if(this.nextJobFlowNode != null){
//            this.nextJobFlowNode.setContainerSequenceJobFlowNodeExecuteContext(containerSequenceJobFlowNodeExecuteContext);
//        }
//    }

    public void setContainerJobFlowContext(JobFlowContext containerJobFlowContext) {
        this.containerJobFlowContext = containerJobFlowContext;
        if(this.nextJobFlowNode != null){
            this.nextJobFlowNode.setContainerJobFlowContext(containerJobFlowContext);
        }
    }

//    public void setContainerJobFlowExecuteContext(JobFlowExecuteContext containerJobFlowExecuteContext) {
//        
//        this.containerJobFlowExecuteContext = containerJobFlowExecuteContext;
//        this.containerJobFlowContext = containerJobFlowExecuteContext.getJobFlowContext();
//        if(this.nextJobFlowNode != null){
//            this.nextJobFlowNode.setContainerJobFlowExecuteContext(containerJobFlowExecuteContext);
//        }
//    }

    public void setContainerParrelJobFlowNodeContext(ParrelJobFlowNodeContext containerParrelJobFlowNodeContext) {
        this.containerParrelJobFlowNodeContext = containerParrelJobFlowNodeContext;

    }

    public CompositionJobFlowNode getCompositionJobFlowNode() {
        return compositionJobFlowNode;
    }

    public void setParentJobFlowNode(JobFlowNode parentJobFlowNode) {
        if(this.parentJobFlowNode != null){
            if(this.parentJobFlowNodeStack == null)
                this.parentJobFlowNodeStack = new Stack<>();
            this.parentJobFlowNodeStack.push(this.parentJobFlowNode);
        }
        this.parentJobFlowNode = parentJobFlowNode;
    }
    
    public void reset(){
        //todo 是否需要reset，jobFlowNodeExecuteContext本身的生命周期就是执行期间有效，执行完后自动销毁，下次创建会重新创建
        if(jobFlowNodeExecuteContext != null){
            jobFlowNodeExecuteContext.reset();
        }
        if(nextJobFlowNode != null){
            nextJobFlowNode.reset();
        }
    }
    
    public boolean assertTrigger(){
        if(nodeTrigger == null){
            if(logger_.isDebugEnabled()) {
                logger_.debug("AssertTrigger: null AssertTrigger and return true,flowNode[id={},name={}].", this.getNodeId(), this.getNodeName());
            }
            return true;
        }
        try {
            if(this.nodeTrigger.assertTrigger(jobFlow,this)){
                if(logger_.isDebugEnabled()) {
                    logger_.debug("AssertTrigger: true,flowNode[id={},name={}].", this.getNodeId(), this.getNodeName());
                }
                return true;
            }
            else{
                if(logger_.isDebugEnabled()) {
                    logger_.debug("AssertTrigger: false,flowNode[id={},name={}].", this.getNodeId(), this.getNodeName());
                }
                return false;
            }
        } catch (Exception e) {
            throw new JobFlowException("AassertTrigger failed:",e);
        }
        catch (Throwable e) {
            throw new JobFlowException("AassertTrigger failed:",e);
        }
    }

    public boolean assertTrigger(String conditionNodeUUID){
        NodeTrigger nodeTrigger = conditionNodeTriggers.get(conditionNodeUUID);
        if(nodeTrigger == null){
            nodeTrigger = this.nodeTrigger;
        }
        if(nodeTrigger == null){
            if(logger_.isDebugEnabled()) {
                logger_.debug("AssertTrigger: null AssertTrigger and return true,flowNode[id={},name={}].", this.getNodeId(), this.getNodeName());
            }
            return true;
        }
        try {
            if(nodeTrigger.assertTrigger(jobFlow,this)){
                if(logger_.isDebugEnabled()) {
                    logger_.debug("AssertTrigger: true,flowNode[id={},name={}].", this.getNodeId(), this.getNodeName());
                }
                return true;
            }
            else{
                if(logger_.isDebugEnabled()) {
                    logger_.debug("AssertTrigger: false,flowNode[id={},name={}].", this.getNodeId(), this.getNodeName());
                }
                return false;
            }
        } catch (Exception e) {
            throw new JobFlowException("AassertTrigger failed:",e);
        }
        catch (Throwable e) {
            throw new JobFlowException("AassertTrigger failed:",e);
        }
    }
    
    

    public JobFlowNode getParentJobFlowNode() {
        return parentJobFlowNode;
    }

    public void setNextJobFlowNode(JobFlowNode nextJobFlowNode) {
        this.nextJobFlowNode = nextJobFlowNode;
    }

    public JobFlowNode getNextJobFlowNode() {
        return nextJobFlowNode;
    }

    public void setNodeTrigger(NodeTrigger nodeTrigger) {
        this.nodeTrigger = nodeTrigger;
    }

    public NodeTrigger getNodeTrigger() {
        return nodeTrigger;
    }

    public void setJobFlow(JobFlow jobFlow) {
        this.jobFlow = jobFlow;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public void setNodeName(String nodeName) {
        this.nodeName = nodeName;
    }

    public String getNodeId() {
        return nodeId;
    }

    public String getNodeName() {
        return nodeName;
    }

    public JobFlow getJobFlow() {
        return jobFlow;
    }

    /**
     * 启动流程当前节点
     */
    public abstract boolean execute(JobFlowNodeExecuteContext jobFlowNodeExecuteContext,JobFlowCyclicBarrier barrier);
    public void execute(JobFlowNodeExecuteContext jobFlowNodeExecuteContext){

        execute(jobFlowNodeExecuteContext,null);
    }
//    /**
//     * 启动流程当前节点
//     */
//    public abstract boolean start();
    /**
     * 停止流程当前节点
     */
    public abstract void stop();

    /**
     * 暂停流程节点
     */
    public abstract void pause();

    /**
     * 唤醒暂停流程节点
     */
    public abstract void consume();
    public void nodeComplete(Throwable throwable){
        nodeComplete(throwable,false);
    }
    /**
     * 作业结束时，节点任务结束,可以唤醒下一个任务
     * 如果没有下一个任务，则检查是否有父节点：
     * 如果有父节点则反向通知父节点，当前节点已经完成任务,可以采取下一步的措施
     * 如果没有父节点，则可能已经到达工作流的第一个节点，也可能到达并行节点的分支起点
     * @param ignoreExecute By NodeTrigger or By Stop 
     */
    
    public void nodeComplete(Throwable throwable,boolean ignoreExecute){
        try {
            if (jobFlowNodeExecuteContext == null || !jobFlowNodeExecuteContext.nodeCompleteUnExecuted()) {
                return;
            }
            jobFlowNodeExecuteContext.setExecuteException(throwable);
            complete(throwable);
            if (throwable != null) {
                logger_.error(this.getJobFlowNodeInfo() + " complete with exception:", throwable);
            }
            if (CollectionUtils.isNotEmpty(this.jobFlowNodeListeners)) {
                for (JobFlowNodeListener jobFlowNodeListener : jobFlowNodeListeners) {
                    try {
                        jobFlowNodeListener.afterExecute(jobFlowNodeExecuteContext, throwable);
                    } catch (Exception e) {
                        logger_.warn(this.getJobFlowNodeInfo() + "JobFlowNodeListener.afterExecute failed:", e);
//                    throw new JobFlowException(this.getJobFlowNodeInfo()+" JobFlowNodeListener.afterExecute failed:",e);
                    }

                }
            }

            release();

            if (this.nextJobFlowNode != null && !ignoreExecute) {
                if(logger_.isDebugEnabled()) {
                    logger_.debug("{} execute complete and start nextJobFlowNode[{}]", getJobFlowNodeInfo(), nextJobFlowNode.getJobFlowNodeInfo());
                }
                JobFlowNodeExecuteContext _jobFlowNodeExecuteContext = nextJobFlowNode.buildJobFlowNodeExecuteContext();
                JobFlowNodeExecuteContext directContainerJobFlowNodeExecuteContext = this.jobFlowNodeExecuteContext.getDirectContainerJobFlowNodeExecuteContext();

                //需将当前节点的实际容器节点执行上下文传递给下一个节点，当前节点隶属于条件节点时，则用条件复合节点的容器执行上下文
                JobFlowNodeExecuteContext tmp = null;
                if(directContainerJobFlowNodeExecuteContext != null && directContainerJobFlowNodeExecuteContext instanceof ConditionJobFlowNodeExecuteContext){
                    tmp = directContainerJobFlowNodeExecuteContext;
                }
                else{
                    tmp = this.jobFlowNodeExecuteContext;
                }

                if (tmp.getContainerJobFlowExecuteContext() != null) {
                    _jobFlowNodeExecuteContext.setContainerJobFlowExecuteContext(tmp.getContainerJobFlowExecuteContext());
                }
                if (tmp.getContainerParrelJobFlowNodeExecuteContext() != null) {
                    _jobFlowNodeExecuteContext.setContainerParrelJobFlowNodeExecuteContext(tmp.getContainerParrelJobFlowNodeExecuteContext());
                }
                if (tmp.getContainerSequenceJobFlowNodeExecuteContext() != null) {
                    _jobFlowNodeExecuteContext.setContainerSequenceJobFlowNodeExecuteContext(tmp.getContainerSequenceJobFlowNodeExecuteContext());
                }


                this.nextJobFlowNode.execute(_jobFlowNodeExecuteContext);
                if(logger_.isDebugEnabled()) {
                    logger_.debug("{} execute complete and nextJobFlowNode[{}] execute completed.", getJobFlowNodeInfo(), nextJobFlowNode.getJobFlowNodeInfo());
                }
            } else {
                JobFlowNodeExecuteContext containerJobFlowNodeExecuteContext = this.jobFlowNodeExecuteContext.getDirectContainerJobFlowNodeExecuteContext();
                if (containerJobFlowNodeExecuteContext != null
//                    || this.jobFlowNodeExecuteContext.getContainerSequenceJobFlowNodeExecuteContext() != null
//                    || this.jobFlowNodeExecuteContext.getContainerParrelJobFlowNodeExecuteContext() != null
//                    || this.jobFlowNodeExecuteContext.getContainerConditionJobFlowNodeExecuteContext() != null
                ) {
                    CompositionJobFlowNode compositionJobFlowNode = (CompositionJobFlowNode) containerJobFlowNodeExecuteContext.getJobFlowNode();
                    if(logger_.isDebugEnabled()) {
                        logger_.debug("Execute {} complete and call compositionJobFlowNode[{}]'s brachComplete.", this.getJobFlowNodeInfo(), compositionJobFlowNode.getJobFlowNodeInfo());
                    }
                    compositionJobFlowNode.brachComplete(this, null);
                } else {
                    if(logger_.isDebugEnabled()) {
                        logger_.debug("Execute {} complete and call {}'s complete.", this.getJobFlowNodeInfo(), jobFlow.getJobInfo());
                    }
                    this.jobFlow.complete(null);
                }


            }
        }
        catch (JobFlowException e){
            logger_.error("Execute nodeComplete failed:"+jobFlow.getJobInfo()+", "+this.getJobFlowNodeInfo(),e);
            throw e;
        }
        catch (Exception e){
            logger_.error("Execute nodeComplete failed:"+jobFlow.getJobInfo()+", "+this.getJobFlowNodeInfo(),e);
            throw new JobFlowException("Execute nodeComplete failed:"+jobFlow.getJobInfo()+", "+this.getJobFlowNodeInfo(),e);
        }
    }
     

    



    

    

    public void setJobFlowNodeType(JobFlowNodeType jobFlowNodeType) {
        this.jobFlowNodeType = jobFlowNodeType;
    }

    public JobFlowNodeType getJobFlowNodeType() {
        return jobFlowNodeType;
    }
    public String toString(){
        return getJobFlowNodeInfo();
    }

    /**
     * 节点启动时，更新工作流、分支（串行/并行)节点运行数量
     */
    protected void nodeStart(){
        JobFlowExecuteContext containerJobFlowExecuteContext = this.jobFlowNodeExecuteContext.getContainerJobFlowExecuteContext();
        if(containerJobFlowExecuteContext != null){
            containerJobFlowContext.setRunningJobFlowNode(this);
            containerJobFlowExecuteContext.nodeStart(this);
            
        }
        SequenceJobFlowNodeExecuteContext containerSequenceJobFlowNodeExecuteContext = this.jobFlowNodeExecuteContext.getContainerSequenceJobFlowNodeExecuteContext();
        if(containerSequenceJobFlowNodeExecuteContext != null){
            containerSequenceJobFlowNodeExecuteContext.setRunningJobFlowNode(this);
            containerSequenceJobFlowNodeExecuteContext.nodeStart(this);
        }
        JobFlowNodeExecuteContext containerParrelJobFlowNodeExecuteContext = this.jobFlowNodeExecuteContext.getContainerParrelJobFlowNodeExecuteContext();
        if(containerParrelJobFlowNodeExecuteContext != null){
            containerParrelJobFlowNodeExecuteContext.nodeStart(this);
        }
    }
    /**
     * 节点完成时，更新工作流、分支（串行/并行)节点完成节点数量
     */
    protected void complete(Throwable throwable){
        JobFlowNodeExecuteContext containerJobFlowNodeExecuteContext = this.jobFlowNodeExecuteContext.getDirectContainerJobFlowNodeExecuteContext();
        if(containerJobFlowNodeExecuteContext != null){
            containerJobFlowNodeExecuteContext.nodeComplete(throwable, this);
        }
        
//        SequenceJobFlowNodeExecuteContext containerSequenceJobFlowNodeExecuteContext = this.jobFlowNodeExecuteContext.getContainerSequenceJobFlowNodeExecuteContext();
//        if(containerSequenceJobFlowNodeExecuteContext != null){
//            containerSequenceJobFlowNodeExecuteContext.nodeComplete( throwable,this);
//        }
        JobFlowExecuteContext containerJobFlowExecuteContext = this.jobFlowNodeExecuteContext.getContainerJobFlowExecuteContext();
        if(containerJobFlowExecuteContext != null){
            containerJobFlowContext.nodeComplete( throwable,this);
            containerJobFlowExecuteContext.nodeComplete(throwable, this);
        }
//        JobFlowNodeExecuteContext containerParrelJobFlowNodeExecuteContext = this.jobFlowNodeExecuteContext.getContainerJobFlowNodeExecuteContext();
//        if(containerParrelJobFlowNodeExecuteContext != null){            
//            containerParrelJobFlowNodeExecuteContext.nodeComplete( throwable,this);
//        }

//        JobFlowNodeExecuteContext containerConditionJobFlowNodeExecuteContext = this.jobFlowNodeExecuteContext.getContainerConditionJobFlowNodeExecuteContext();
//        if(containerConditionJobFlowNodeExecuteContext != null){
//            containerConditionJobFlowNodeExecuteContext.nodeComplete( throwable,this);
//        }
    }

    /**
     * 一个节点可能包含在多个条件节点中
     * @param conditionJobFlowNode
     */
    protected void addConditionJobFlowNode(ConditionJobFlowNode conditionJobFlowNode) {
        if(this.conditionJobFlowNodes == null){
            this.conditionJobFlowNodes = new ArrayList<>();
            
        }
        this.conditionJobFlowNodes.add(conditionJobFlowNode);
    }

    public List<ConditionJobFlowNode> getConditionJobFlowNodes() {
        return conditionJobFlowNodes;
    }

    public void setRunningConditionJobFlowNode(ConditionJobFlowNode runningConditionJobFlowNode) {
        this.runningConditionJobFlowNode = runningConditionJobFlowNode;
    }

    public ConditionJobFlowNode getRunningConditionJobFlowNode() {
        return runningConditionJobFlowNode;
    }

    public void addConditionNodeTrigger(String conditionJobFlowNodeUUID, NodeTrigger conditionNodeTrigger) {
        this.conditionNodeTriggers.put(conditionJobFlowNodeUUID,conditionNodeTrigger);
    }
    public NodeTrigger getConditionNodeTrigger(String conditionJobFlowNodeUUID) {
        return this.conditionNodeTriggers.get(conditionJobFlowNodeUUID);
    }

  
}
