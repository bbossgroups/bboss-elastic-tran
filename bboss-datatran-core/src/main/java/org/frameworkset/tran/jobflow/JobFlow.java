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
import org.codehaus.groovy.control.CompilerConfiguration;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.jobflow.schedule.JobFlowScheduleConfig;
import org.frameworkset.tran.jobflow.schedule.JobFlowScheduleTimer;
import org.frameworkset.tran.schedule.ScheduleEndCall;
import org.frameworkset.tran.schedule.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;

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
    private JobFlowStatus jobFlowStatus = JobFlowStatus.INIT;
    /**
     * 作业流程名称
     */
    private String jobFlowName;

   

    private JobFlowScheduleConfig jobFlowScheduleConfig;
    /**
     * 流程的首节点
     */
    private JobFlowNode startJobFlowNode;
    private JobFlowScheduleTimer jobFlowScheduleTimer;
    private List<SimpleJobFlowNode> jobFlowNodes;

    private JobFlowExecuteContext jobFlowExecuteContext;

    private GroovyClassLoader groovyClassLoader ;

    public GroovyClassLoader getGroovyClassLoader() {
        return groovyClassLoader;
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
    }

    private String jobInfo ;
    public void initJobInfo(){
        StringBuilder info = new StringBuilder();
        info.append("JobFlow[jobFlowId=").append(this.getJobFlowId()).append(",jobFlowName=").append(this.getJobFlowName()).append("]");
        jobInfo = info.toString();
    }
    public void execute(){
        logger.info("Execute "+jobInfo );
        
        startEndScheduleThread(new ScheduleEndCall() {
            @Override
            public void call(boolean scheduled) {
                stop(true);
            }
        });
        this.startJobFlowNode.start();
    }

    private Object startEndScheduleThreadLock = new Object();
    private Thread scheduledEndThread;
    /**
     * 启动作业自动结束线程
     * @param scheduleEndCall
     */
    protected void startEndScheduleThread( ScheduleEndCall scheduleEndCall){
        Date scheduleEndDate = jobFlowScheduleConfig.getScheduleEndDate();
        if(scheduleEndDate != null){

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
    private Object statusChangeLock = new Object();
    
    public void initJob(){
        synchronized (statusChangeLock) {
            if(jobFlowStatus == JobFlowStatus.STARTED){
                return;
            }           
            initGroovyClassLoader();
            JobFlowExecuteContext jobFlowExecuteContext = new DefaultJobFlowExecuteContext();
            this.jobFlowExecuteContext = jobFlowExecuteContext;
            jobFlowStatus = JobFlowStatus.STARTED;
        }
    }
    /**
     * 启动工作流
     */
    public void start(){
        
       
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
        synchronized (statusChangeLock) {
            //作业未启动
            if(jobFlowStatus == JobFlowStatus.INIT){
                jobFlowStatus = JobFlowStatus.STOPED;
                logger.info("Stop unstarted jobflow {} [fromScheduled={}] complete.", this.jobInfo, fromScheduled);
                return;
            }
            if (jobFlowStatus == JobFlowStatus.STARTED && jobFlowStatus != JobFlowStatus.STOPPING) {

                jobFlowStatus = JobFlowStatus.STOPPING;
                logger.info("Stop {} [fromScheduled={}] start.", this.jobInfo, fromScheduled);
                this.startJobFlowNode.stop();
                try {
                    this.jobFlowScheduleTimer.stop();
                } catch (Exception e) {
                }
                try {
                    if (groovyClassLoader != null) {
                        groovyClassLoader.clearCache();
                    }
                } catch (Exception e) {
                }
                if (!fromScheduled) {
                    if (this.scheduledEndThread != null) {
                        try {
                            this.scheduledEndThread.interrupt();
                            this.scheduledEndThread.join();
                        } catch (Exception e) {

                        }
                    }
                }
                jobFlowStatus = JobFlowStatus.STOPED;
                logger.info("Stop {} [fromScheduled={}] complete.", this.jobInfo, fromScheduled);
            }
            else{
                if(jobFlowStatus == JobFlowStatus.STOPPING){
                    logger.info("Jobflow {} [fromScheduled={}] STOPPING.", this.jobInfo, fromScheduled);
                }
                else{
                    logger.info("Jobflow {} [fromScheduled={}] stoped,ignore stop operation.", this.jobInfo, fromScheduled);
                }
            }
        }
            
    }

    /**
     * 暂停工作流
     */
    public void pause(){
        this.startJobFlowNode.pause();
    }

    /**
     * 唤醒工作流
     */
    public void consume(){
        this.startJobFlowNode.consume();
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
    public void complete(ImportContext importContext, Throwable e) {
        this.jobFlowExecuteContext.increamentNums();
        this.jobFlowExecuteContext.clear();
    }

    /**
     * 作业结束时触发工作流任务结束回调方法，等待下一次任务的调度，如果是一次性任务，则直接结束流程任务
     */
    public void complete(Throwable e) {
        this.jobFlowExecuteContext.increamentNums();
        this.jobFlowExecuteContext.clear();
    }

    /**
     * 每次作业调度任务迭代结束时触发流程结束回调方法：等待下一次任务的调度，如果是一次性任务，则直接结束流程任务
     */
    public void complete(TaskContext taskContext, Throwable e) {
        this.jobFlowExecuteContext.increamentNums();
        this.jobFlowExecuteContext.clear();
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
}
