package org.frameworkset.tran.jobflow.context;
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

import org.frameworkset.tran.jobflow.JobFlow;
import org.frameworkset.tran.jobflow.JobFlowNode;
import org.frameworkset.tran.jobflow.JobFlowStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.function.Function;

/**
 * 跟踪和记录工作流节点执行情况
 * @author biaoping.yin
 * @Date 2025/6/18
 */
public class JobFlowContext  extends StaticContext{
    private static Logger logger = LoggerFactory.getLogger(JobFlowContext.class);
    private JobFlow jobFlow;
    protected JobFlowStatus jobFlowStatus = JobFlowStatus.INIT;
    /**
     * 当前正在执行的作业节点
     */
    private JobFlowNode runningJobFlowNode;

    private Object runningJobFlowNodeLock = new Object();
    public JobFlowContext(JobFlow jobFlow){
        super();
        this.jobFlow = jobFlow;
    }
    public JobFlow getJobFlow() {
        return jobFlow;
    }
    private Object updateJobFlowStatusLock = new Object();
    public JobFlowStatus updateJobFlowStatus(JobFlowStatus jobFlowStatus){
        synchronized (updateJobFlowStatusLock){
            this.jobFlowStatus = jobFlowStatus;
            return jobFlowStatus;
        }
       
    }
    
    public Object getUpdateJobFlowStatusLock(){
        return updateJobFlowStatusLock;
    }

    /**
     * 判断作业是否已经启动过
     * @return
     */
    public AssertResult assertStarted(){
        synchronized (updateJobFlowStatusLock){
            return new AssertResult(jobFlowStatus,jobFlowStatus != JobFlowStatus.INIT )    ;
        }
    }

    /**
     * 判断作业是否已经停止或者正在停止中
     * @return
     */
    public AssertResult assertStopped(){
        synchronized (updateJobFlowStatusLock){
            return new AssertResult(jobFlowStatus,jobFlowStatus == JobFlowStatus.STOPED
                    || jobFlowStatus == JobFlowStatus.STOPPING )    ;
        }
    }

    /**
     * 判断作业当前状态是否与输入状态一至
     * @return
     */
    public AssertResult assertStatus(JobFlowStatus ... jobFlowStatus ){
        synchronized (updateJobFlowStatusLock){
            boolean result = false;
            for(JobFlowStatus tmp: jobFlowStatus){
                if(this.jobFlowStatus == tmp){
                    result = true;
                    break;
                }
            }
            return new AssertResult(this.jobFlowStatus,result )    ;
        }
    }
    public JobFlowStatus initJob(){
        synchronized (updateJobFlowStatusLock){
            if(jobFlowStatus == JobFlowStatus.INIT  ){
                jobFlowStatus = JobFlowStatus.STARTED;
            }
            return jobFlowStatus;
        }
        
    }

    public JobFlowStatus getJobFlowStatus() {
        synchronized (updateJobFlowStatusLock){
            return jobFlowStatus;
        }
        
    }

    public void setRunningJobFlowNode(JobFlowNode runningJobFlowNode) {
        synchronized (runningJobFlowNodeLock) {
            this.runningJobFlowNode = runningJobFlowNode;
        }
    }

    public JobFlowNode getRunningJobFlowNode() {
        synchronized (runningJobFlowNodeLock) {
            return runningJobFlowNode;
        }
    }

    public boolean stop(Function callback,boolean fromScheduled) {
        boolean result = false;
        synchronized (runningJobFlowNodeLock) {
            AssertResult assertResult = this.assertStatus(JobFlowStatus.COMPLETE,JobFlowStatus.RUNNING,
                                                    JobFlowStatus.STARTED,JobFlowStatus.PAUSE);
            if(assertResult.isFalse()){
                logger.info("Stop {} [fromScheduled={}] 处于非运行状态:{}，忽略停止操作.", jobFlow.getJobInfo(), fromScheduled,assertResult.getJobFlowStatus().name());
                return result;
            }
            logger.info("Stop {} [fromScheduled={}] start.", jobFlow.getJobInfo(), fromScheduled);
            updateJobFlowStatus(JobFlowStatus.STOPPING);
        }
        /**
         * 工作流停止，并且中断暂停状态
         */
        stopAndInteruptPause();
        if(runningJobFlowNode != null) {               
            runningJobFlowNode.stop();                
        }
        else{
            logger.info("Stop {} [fromScheduled={}] :ignore stop runningJobFlowNode,runningJobFlowNode is null.", jobFlow.getJobInfo(), fromScheduled);
        }
        
        callback.apply(null);
        synchronized (runningJobFlowNodeLock) {
            updateJobFlowStatus(JobFlowStatus.STOPED);
            result = true;
        }
        logger.info("Stop {} [fromScheduled={}] complete.", jobFlow.getJobInfo(), fromScheduled);
        
        
        return result;
        
    }

    private CountDownLatch pauseCountDownLatch = null;
    public boolean pause() {
        boolean result = false;
        synchronized (runningJobFlowNodeLock) {
            AssertResult assertResult = this.assertStatus(JobFlowStatus.COMPLETE,JobFlowStatus.RUNNING,JobFlowStatus.STARTED);
            if(assertResult.isFalse()){
                logger.warn("{} 处于状态:{},忽略暂停操作.",jobFlow.getJobInfo(),assertResult.getJobFlowStatus().name());
                
            }
            else {
                
                logger.warn("Pause {} start.", jobFlow.getJobInfo(), assertResult.getJobFlowStatus().name());
                if (runningJobFlowNode != null) {
                    runningJobFlowNode.pause();  
                }
                else{
                    logger.info("Pause {} :ignore pause runningJobFlowNode,runningJobFlowNode is null,", jobFlow.getJobInfo());
                }
                synchronized (pauseCountDownLatchLock) {
                    pauseCountDownLatch = new CountDownLatch(1);
                }
                updateJobFlowStatus(JobFlowStatus.PAUSE);
                result = true;
                logger.warn("Pause {} complete.",jobFlow.getJobInfo(),assertResult.getJobFlowStatus().name());
            }
            
            
        }
        return result;
        
    }
    
    private Object pauseCountDownLatchLock = new Object();
    public void pauseAwait(){
        pauseAwait(null);
    }
    public void pauseAwait(JobFlowNode jobFlowNode){
        if(assertStatus(JobFlowStatus.PAUSE).isTrue() ){
            try {
                CountDownLatch countDownLatch = null;
                synchronized (pauseCountDownLatchLock){
                    countDownLatch = this.pauseCountDownLatch;
                }
                if(countDownLatch != null) {
                    String info = null;
                    if(jobFlowNode != null){
                        info = jobFlowNode.getJobFlowNodeInfo();
                    }
                    else{
                        info = jobFlow.getJobInfo();
                    }
                    logger.info("Pause {} begin.",info);
                    countDownLatch.await();
                    logger.info("Consume {} complete.",info);
                }
            } catch (InterruptedException e) {
//                throw new RuntimeException(e);
            }
        }
    }

    /**
     * 工作流停止，并且中断暂停状态
     */
    public void stopAndInteruptPause(){
        pauseLatchCountDown();
    }
    protected void pauseLatchCountDown(){
        synchronized (pauseCountDownLatchLock) {
            if(pauseCountDownLatch != null) {
                pauseCountDownLatch.countDown();
            }
        }
    }
    public boolean consume() {

        boolean result = false;
        synchronized (runningJobFlowNodeLock) {
            AssertResult assertResult = this.assertStatus(JobFlowStatus.PAUSE);
            if(assertResult.isFalse()){
                logger.warn("{} 工作流处于非暂停状态：{}，忽略Consume操作",jobFlow.getJobInfo(),assertResult.getJobFlowStatus().name());
                
            }
            else {
                pauseLatchCountDown();
                if (runningJobFlowNode != null) {
                    runningJobFlowNode.consume();                    
                }
                else{
                    logger.info("Consume {} :ignore consume runningJobFlowNode,runningJobFlowNode is null,", jobFlow.getJobInfo());
                }
                updateJobFlowStatus(JobFlowStatus.RUNNING);
                result = true;
            }

            
            
        }
        return result;
        
    }
    /**
     * 工作流或者复合节点（串行/并行）子节点完成时，减少启动节点计数,完成计数器加1
     * @param throwable 子节点触发的异常
     * @param jobFlowNode 完成的子节点
     * @return
     */    
    @Override
    public int nodeComplete(Throwable throwable, JobFlowNode jobFlowNode) {
        this.runningJobFlowNode = null;
        return super.nodeComplete(throwable,jobFlowNode);
        
    }

}
