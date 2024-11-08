package org.frameworkset.tran.metrics;
/**
 * Copyright 2024 bboss
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

import org.frameworkset.tran.schedule.TaskContext;

/**
 * <p>Description: 用于在作业过程中记录作业和作业任务中的日志信息</p>
 * <p></p>
 *
 * @author biaoping.yin
 * @Date 2024/10/14
 */
public interface MetricsLogReport {
    /**
     * 记录作业处理过程中的异常日志
     * @param taskContext
     * @param msg
     * @param e
     */
    public void reportJobMetricErrorLog(TaskContext taskContext, String msg, Throwable e);

    /**
     * 记录作业处理过程中的一般日志
     * @param taskContext
     * @param msg
     */
    public void reportJobMetricLog(  TaskContext taskContext,String msg);

    /**
     * 记录作业处理过程中的告警日志
     * @param taskContext
     * @param msg
     */
      public void reportJobMetricWarn(TaskContext taskContext,String msg);

    /**
     * 记录作业处理过程中的异常日志
     * @param taskMetrics
     * @param msg
     * @param e
     */
    public void reportTaskMetricErrorLog(TaskMetrics taskMetrics, String msg, Throwable e);

    /**
     * 记录作业处理过程中的一般日志
     * @param taskMetrics
     * @param msg
     */
    public void reportTaskMetricLog(TaskMetrics taskMetrics,String msg);

    /**
     * 记录作业处理过程中的告警日志
     * @param msg
     */
     public void reportTaskMetricWarn( TaskMetrics taskMetrics, String msg);

    /**
     * 记录作业处理过程中的debug日志
     * @param taskContext
     * @param msg
     */
    void reportJobMetricDebug(TaskContext taskContext, String msg);

    /**
     * 记录作业子任务处理过程中的debug日志
     * @param taskMetrics
     * @param msg
     */
    void reportTaskMetricDebug(TaskMetrics taskMetrics, String msg);
}
