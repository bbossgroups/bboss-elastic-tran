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
 * <p>Description: </p>
 * <p></p>
 *
 * @author biaoping.yin
 * @Date 2024/10/15
 */
public interface DataTranPluginMetricsLogAPI {
    /**
     * 记录作业处理过程中的异常日志
     * @param taskContext
     * @param msg
     * @param e
     */
    default public void reportJobMetricErrorLog(TaskContext taskContext, String msg, Throwable e){

    }



    /**
     * 记录作业处理过程中的日志
     * @param taskContext
     * @param msg
     */
    default public void reportJobMetricLog(TaskContext taskContext,String msg){

    }

    /**
     * 记录作业处理过程中的日志
     * @param taskContext
     * @param msg
     */
    default public void reportJobMetricWarn( TaskContext taskContext,String msg){

    }


    /**
     * 记录作业任务处理过程中的异常日志
     * @param taskMetrics
     * @param msg
     * @param e
     */
    default public void reportTaskMetricErrorLog(TaskMetrics taskMetrics, String msg, Throwable e){

    }



    /**
     * 记录作业任务处理过程中的日志
     * @param taskMetrics
     * @param msg
     */
    default public void reportTaskMetricLog(TaskMetrics taskMetrics, String msg){

    }

    /**
     * 记录作业任务处理过程中的日志
     * @param taskMetrics
     * @param msg
     */
    default public void reportTaskMetricWarn(TaskMetrics taskMetrics,String msg){

    }
}
