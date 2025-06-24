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


import groovy.lang.GroovyClassLoader;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.codehaus.groovy.control.CompilerConfiguration;
import org.frameworkset.tran.jobflow.context.AssertResult;
import org.frameworkset.tran.jobflow.context.DefaultJobFlowExecuteContext;
import org.frameworkset.tran.jobflow.context.JobFlowContext;
import org.frameworkset.tran.jobflow.context.JobFlowExecuteContext;
import org.frameworkset.tran.jobflow.listener.JobFlowListener;
import org.frameworkset.tran.jobflow.metrics.JobFlowMetrics;
import org.frameworkset.tran.jobflow.schedule.JobFlowScheduleConfig;
import org.frameworkset.tran.jobflow.schedule.JobFlowScheduleTimer;
import org.frameworkset.tran.schedule.ScheduleEndCall;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.function.Function;

import static java.lang.Thread.sleep;

/**
 * <p>Description: 作业任务编排流程</p>
 * <p></p>
 *
 * @author biaoping.yin
 * @Date 2025/3/31
 */
public class JobFlow {
    private static final Logger logger = LoggerFactory.getLogger(JobFlow.class);
 
    /**
     * 作业流程id
     */
    private String jobFlowId;
//    private JobFlowStatus jobFlowStatus = JobFlowStatus.INIT;
    /**
     * 作业流程名称
     */
    private String jobFlowName;

   

    private JobFlowScheduleConfig jobFlowScheduleConfig;
    private boolean externalTimer;
    /**
     * 流程的首节点
     */
    private JobFlowNode startJobFlowNode;
    private JobFlowScheduleTimer jobFlowScheduleTimer;

    private JobFlowExecuteContext jobFlowExecuteContext;
    private JobFlowContext jobFlowContext;
    private JobFlowMetrics jobFlowMetrics;
    private GroovyClassLoader groovyClassLoader ;
    private List<JobFlowListener> jobFlowListeners;

    public JobFlow(){
        jobFlowContext = new JobFlowContext(this);
        initGroovyClassLoader();
        
        jobFlowMetrics = new JobFlowMetrics();
    }

    public JobFlowMetrics getJobFlowMetrics() {
        return jobFlowMetrics;
    }

    public void setJobFlowListeners(List<JobFlowListener> jobFlowListeners) {
        this.jobFlowListeners = jobFlowListeners;
    }

    public List<JobFlowListener> getJobFlowListeners() {
        return jobFlowListeners;
    }

    public GroovyClassLoader getGroovyClassLoader() {
        return groovyClassLoader;
    }

    public JobFlowContext getJobFlowContext() {
        return jobFlowContext;
    }

    private  void initGroovyClassLoader() {
        if(groovyClassLoader != null){
            return;
        }
        CompilerConfiguration config = new CompilerConfiguration();
        config.setSourceEncoding("UTF-8");
        // 设置该GroovyClassLoader的父ClassLoader为当前线程的加载器(默认)
        groovyClassLoader = new GroovyClassLoader(Thread.currentThread().getContextClassLoader(), config);

        logger.info("initGroovyClassLoader with SourceEncoding UTF-8,"+jobInfo );
    }

    public void setStartJobFlowNode(JobFlowNode startJobFlowNode) {
        this.startJobFlowNode = startJobFlowNode;
        this.startJobFlowNode.setContainerJobFlowContext(this.jobFlowContext);
    }

    private String jobInfo ;
    public void initJobInfo(){
        StringBuilder info = new StringBuilder();
        info.append("JobFlow[jobFlowId=").append(this.getJobFlowId()).append(",jobFlowName=").append(this.getJobFlowName()).append("]");
        jobInfo = info.toString();
    }
    public void execute(){
        logger.info("Execute {} begin.",jobInfo );
       
        startEndScheduleThread(new ScheduleEndCall() {
            @Override
            public void call(boolean scheduled) {
                stop(true);
            }
        });
        
       
        reset();
        this.jobFlowExecuteContext = new DefaultJobFlowExecuteContext(this);
        jobFlowMetrics.addTotalCount();
        if(CollectionUtils.isNotEmpty(this.jobFlowListeners)){
            for(JobFlowListener jobFlowListener:jobFlowListeners){
                jobFlowListener.beforeExecute(jobFlowExecuteContext);
            }
        }
        Throwable throwable = null;
        try {
            this.startJobFlowNode.start();
        }
        catch (RuntimeException e){
            throwable = e;
            throw e;
        }
        catch (Exception e){
            throwable = e;
            throw new JobFlowException(e);
        }
        catch (Throwable e){
            throwable = e;
            throw new JobFlowException(e);
        }
        finally {
            
        }
    }


    private Object startEndScheduleThreadLock = new Object();
    private Thread scheduledEndThread;
    /**
     * 启动作业自动结束线程
     * @param scheduleEndCall
     */
    protected void startEndScheduleThread( ScheduleEndCall scheduleEndCall){
        if(jobFlowScheduleConfig == null)
            return;
        Date scheduleEndDate = jobFlowScheduleConfig.getScheduleEndDate();
        if(scheduleEndDate != null){
            if(jobFlowScheduleConfig.isExecuteOneTime()){
                logger.info("一次性执行工作流，忽略Start EndSchedule Thread：scheduleEndDate[{}]",DateFormatUtils.format(scheduleEndDate,"yyyy-MM-dd HH:mm:ss.SSS"));
                return;
            }
            synchronized (startEndScheduleThreadLock) {
                if(scheduledEndThread == null) {
                    final long waitTime = scheduleEndDate.getTime() - System.currentTimeMillis();

                    scheduledEndThread = new Thread(new Runnable() {
                        @Override
                        public void run() {
                            if (waitTime > 0) {
                                try {
                                    sleep(waitTime);
                                    scheduleEndCall.call(true);
                                } catch (InterruptedException e) {

                                }
                            } else {
                                scheduleEndCall.call(true);
                            }

                        }
                    }, "Datatran-JobFlowScheduledEndThread");
                    scheduledEndThread.setDaemon(true);
                    scheduledEndThread.start();
                }
            }
        }
    }
    /**
     * 作业工作流每次调度执行时，重置工作流执行状态
     */
    private void reset(){
        this.jobFlowContext.reset();
        this.startJobFlowNode.reset();
    }

    public void initJob(){
        jobFlowContext.initJob();            
    }
    /**
     * 启动工作流
     */
    public void start(){
        
       
        //判断作业是否已经启动
        AssertResult assertResult = jobFlowContext.assertStarted();
        if(assertResult.isTrue()){
            logger.warn("{} 状态为：{}，已经启动过，忽略启动操作。",jobInfo,assertResult.getJobFlowStatus().name());
            return;
        }
        if(CollectionUtils.isNotEmpty(this.jobFlowListeners)){
            for(JobFlowListener jobFlowListener:jobFlowListeners){
                jobFlowListener.beforeStart(this);
            }
        }
        //一次性执行，定时执行处理
        if(jobFlowScheduleConfig == null || jobFlowScheduleConfig.isExecuteOneTime() ){
            initJob();
            this.execute();
        }
        else if(!jobFlowScheduleConfig.isExternalTimer()){
            initJob();         
            JobFlowScheduleTimer jobFlowScheduleTimer = new JobFlowScheduleTimer(jobFlowScheduleConfig,this);
            jobFlowScheduleTimer.start();
            this.jobFlowScheduleTimer = jobFlowScheduleTimer;
        }
        else{
            logger.info("JobFlow is scheduled by externalTimer,ignore start.");
        }
        
    }

    public String getJobInfo() {
        return jobInfo;
    }

    private void release(){
        if(groovyClassLoader != null) {
            groovyClassLoader.clearCache();
        }
    }

    /**
     * 停止工作流
     */
    public void stop(){
        stop(false);

    }
    
    
    /**
     * 停止工作流
     * @param fromScheduled 标记工作流停止操作是否是因为结束日期到达后触发 true 是 false 否
     */
    protected void stop(boolean fromScheduled){
        
        
//                this.startJobFlowNode.stop();
        boolean stopResult = this.jobFlowContext.stop(new Function() {
            @Override
            public Object apply(Object o) {
                startJobFlowNode.stop();
                try {
                    jobFlowScheduleTimer.stop();
                } catch (Exception e) {
                }
                try {
                    if (groovyClassLoader != null) {
                        groovyClassLoader.clearCache();
                    }
                } catch (Exception e) {
                }
                if (!fromScheduled) {
                    if (scheduledEndThread != null) {
                        try {
                            scheduledEndThread.interrupt();
                            scheduledEndThread.join();
                        } catch (Exception e) {

                        }
                    }
                }
                return null;
            }
        }, fromScheduled);

        if(CollectionUtils.isNotEmpty(this.jobFlowListeners)){
            for(JobFlowListener jobFlowListener:jobFlowListeners){
                jobFlowListener.afterEnd(this);
            }
        }
        
         
             
        
            
    }

    /**
     * 暂停工作流
     */
    public void pause(){
//        this.startJobFlowNode.pause();
        this.jobFlowContext.pause();
    }

    /**
     * 唤醒工作流
     */
    public void consume(){
        this.jobFlowContext.consume();
    }

    public JobFlowExecuteContext getJobFlowExecuteContext() {
        return jobFlowExecuteContext;
    }

    public String getJobFlowId() {
        return jobFlowId;
    }

    public void setJobFlowId(String jobFlowId) {
        this.jobFlowId = jobFlowId;
    }

    public String getJobFlowName() {
        return jobFlowName;
    }

    public void setJobFlowName(String jobFlowName) {
        this.jobFlowName = jobFlowName;
    }

    public void setJobFlowExecuteContext(JobFlowExecuteContext jobFlowExecuteContext) {
        this.jobFlowExecuteContext = jobFlowExecuteContext;
    }

 

 

 

    /**
     * 作业结束时触发工作流任务结束回调方法，等待下一次任务的调度，如果是一次性任务，则直接结束流程任务
     */
    public void complete(Throwable throwable){
        jobFlowMetrics.complete(throwable);
        if(isExternalTimer()){
            //周期性执行，更新状态为调度一次完成
            this.jobFlowContext.updateJobFlowStatus(JobFlowStatus.COMPLETE);
            logger.info("{} 调度执行完成，更新工作流状态为调度完成",jobInfo);
        }
        else if(jobFlowScheduleConfig == null || jobFlowScheduleConfig.isExecuteOneTime()){
            //一次性执行，更新状态为停止
            this.jobFlowContext.updateJobFlowStatus(JobFlowStatus.STOPED);
            logger.info("{} 一次性执行完成，更新工作流状态为停止",jobInfo);
        }
        else {
            //周期性执行，更新状态为调度一次完成
            this.jobFlowContext.updateJobFlowStatus(JobFlowStatus.COMPLETE);
            logger.info("{} 调度执行完成，更新工作流状态为调度完成",jobInfo);
        }
        if(CollectionUtils.isNotEmpty(this.jobFlowListeners)){
            for(JobFlowListener jobFlowListener:jobFlowListeners){
                jobFlowListener.afterExecute(jobFlowExecuteContext,throwable);
            }
        }
        this.jobFlowExecuteContext = null;
        this.reset();
    }

    public boolean isEnableAutoPauseScheduled() {
        return false;
    }

    public boolean isSchedulePaused(boolean enableAutoPauseScheduled) {
        return false;
    }

    public JobFlowScheduleConfig getJobScheduleConfig() {
        return jobFlowScheduleConfig;
    }

    public void setJobScheduleConfig(JobFlowScheduleConfig jobFlowScheduleConfig) {
        this.jobFlowScheduleConfig = jobFlowScheduleConfig;
    }
    
    public String toString(){
        return this.jobInfo;
    }

    public boolean isExternalTimer() {
        return externalTimer;
    }

    public void setExternalTimer(boolean externalTimer) {
        this.externalTimer = externalTimer;
    }
}
