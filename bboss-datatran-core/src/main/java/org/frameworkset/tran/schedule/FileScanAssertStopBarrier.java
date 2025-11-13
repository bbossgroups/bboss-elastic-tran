package org.frameworkset.tran.schedule;
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

import org.frameworkset.tran.jobflow.context.JobFlowNodeExecuteContext;

/**
 * 一次性执行扫描任务是否终止决断信号器，如果扫描任务需要终止则返回true，否则返回false
 * 如果返回false，需要等待一会，然后再次执行扫描采集任务,直到返回true时终止一次性采集任务
 * @author biaoping.yin
 * @Date 2025/11/13
 */
public abstract class FileScanAssertStopBarrier implements AssertStopBarrier{
    protected long sleepTime;
    protected JobFlowNodeExecuteContext jobFlowNodeExecuteContext;
    public FileScanAssertStopBarrier(JobFlowNodeExecuteContext jobFlowNodeExecuteContext, long sleepTime){
        this.jobFlowNodeExecuteContext = jobFlowNodeExecuteContext;
        this.sleepTime = sleepTime;
    }

    public FileScanAssertStopBarrier(JobFlowNodeExecuteContext jobFlowNodeExecuteContext){
        this(jobFlowNodeExecuteContext,5000L);
    }

    /**
     * 一次性执行扫描任务是否终止决断信号器，如果扫描任务需要终止则返回true，否则返回false
     * 如果返回false，需要等待一会，然后再次执行扫描采集任务,直到返回true时终止一次性采集任务
     * @return
     */
    protected abstract boolean canStop();
    /**
     * 一次性执行扫描任务是否终止决断信号器，如果扫描任务需要终止则返回true，否则返回false
     * 如果返回false，需要等待一会，然后再次执行扫描采集任务,直到返回true时终止一次性采集任务
     * @return
     */
    @Override
    public boolean assertStop() {
        if(canStop()){
            return true;
        }
        else{
            try {
                if(sleepTime > 0L) {
                    Thread.sleep(sleepTime);
                }
                return false;
            } catch (InterruptedException e) {
                return true;
            }
        }
    }
}
