package org.frameworkset.tran.jobflow.schedule;
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
import org.frameworkset.tran.jobflow.JobFlowExecuteContext;
import org.frameworkset.tran.schedule.timer.TimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

/**
 * job工作流调度器，作业中节点如果配置了定时调度器，将失效，已作业调度器为准
 * @author biaoping.yin
 * @Date 2025/6/10
 */
public class JobFlowScheduleTimer implements Runnable{
    private JobFlowScheduleConfig jobFlowScheduleConfig;
    private JobFlow jobFlow;
    private JobFlowExecuteContext jobFlowExecuteContext;
    private static Logger logger = LoggerFactory.getLogger(JobFlowScheduleTimer.class);
    private Thread thread = null;
    protected volatile boolean running = false;
    public JobFlowScheduleTimer(JobFlowScheduleConfig jobFlowScheduleConfig, JobFlow jobFlow ){
        this.jobFlowScheduleConfig = jobFlowScheduleConfig;
        this.jobFlow = jobFlow;
        this.jobFlowExecuteContext = jobFlow.getJobFlowExecuteContext();
    }

    /**
     * Starts monitoring.
     *
     * @throws Exception if an error occurs initializing the observer
     */
    public synchronized void start() throws IllegalStateException {
        if (running) {
            throw new IllegalStateException("BBossJobScheduleTimer is already running");
        }

        running = true;
        logger.info("Start JobFlow BBossJobScheduleTimer,JobFlowScheduleConfig[{}].",jobFlowScheduleConfig.toString());
        thread = new Thread(this,"BBossJobScheduleTimer-"+jobFlow.getJobFlowName());
        thread.setDaemon(false);
        thread.start();
    }

    /**
     * Stops monitoring.
     *
     * @throws Exception if an error occurs initializing the observer
     */
    public synchronized void stop() throws Exception {
        stop(jobFlowScheduleConfig.getPeriod());
    }

    /**
     * Stops monitoring.
     *
     * @param stopInterval the amount of time in milliseconds to wait for the thread to finish.
     * A value of zero will wait until the thread is finished (see {@link Thread#join(long)}).
     * @throws Exception if an error occurs initializing the observer
     * @since 2.1
     */
    public synchronized void stop(final long stopInterval) throws Exception {
        if (running == false) {
            throw new IllegalStateException("Monitor is not running");
        }
        running = false;
        try {
            thread.interrupt();
            thread.join(stopInterval);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
    private Object runLock = new Object();
    /**
     * Runs this monitor.
     */
    @Override
    public void run() {
        Long interval = jobFlowScheduleConfig.getPeriod();
        if(interval == null){
            interval = 100000l;
        }
        //第一次执行时，设置延时时间
        Long _deLay = null;
        Long deyLay = jobFlowScheduleConfig.getDelay();
        
        
        Date scheduleDate = jobFlowScheduleConfig.getScheduleDate();

        if (scheduleDate != null) {
            Date now = new Date();
            if (scheduleDate.after(now)) {
                _deLay = scheduleDate.getTime() - now.getTime();
            }
        }
        if(_deLay == null && deyLay != null){
            _deLay = deyLay;
        }
        
         
        if(_deLay != null){
            try {
                Thread.sleep(_deLay);
            } catch (final InterruptedException ignored) {
                return;
            }
        }
        while (running) {
            /**
             * 如果没有到达执行时间点，则定时检查直到命中扫描时间点
             */
            Exception exception = null;
            do {

                if (TimeUtil.evalateNeedScan(jobFlowScheduleConfig)) {
                    if(jobFlow.isSchedulePaused(jobFlow.isEnableAutoPauseScheduled())){
                        if(logger.isInfoEnabled()){
                            logger.info("Ignore  paused schedule job,waiting for next resume schedule sign to continue.");
                        }
                        break;
                    }
                    synchronized (runLock) {
                        try {
                            jobFlow.execute();
                        }
                        catch (Exception e){
                            exception = e;
                        }
                    }
                    break;
                }
                else {
                    try {
                        Thread.sleep(1000l);
                    } catch (final InterruptedException ignored) {
                        // ignore
                        break;
                    }
                }
            }while(true);
            if (!running) {
                logger.warn("running=false and end JobFlowScheduleTimer.");
                break;
            }
            if(exception != null ){
                if(!jobFlowScheduleConfig.isContinueOnError()) {
                    logger.warn("Exception occur and continueOnError is false,so end JobFlowScheduleTimer.", exception);
                    break;
                }
                else{
                    logger.warn("Exception occur and continueOnError is true,so continue JobFlowScheduleTimer.", exception);
                }
            }
            try {
                Thread.sleep(interval);
            } catch (final InterruptedException ignored) {
                // ignore
            }
        }
    }

}
