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

import org.frameworkset.tran.DataTranPlugin;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.task.TaskCommand;

/**
 * <p>Description: </p>
 * <p></p>
 *
 * @author biaoping.yin
 * @Date 2024/10/15
 */
public abstract class BaseMetricsLogReport implements DataTranPluginMetricsLogAPI {
    protected DataTranPlugin dataTranPlugin;
    public BaseMetricsLogReport(DataTranPlugin dataTranPlugin){
        this.dataTranPlugin = dataTranPlugin;
    }

    public BaseMetricsLogReport(){
    }

    public void setDataTranPlugin(DataTranPlugin dataTranPlugin) {
        this.dataTranPlugin = dataTranPlugin;
    }

    public DataTranPlugin getDataTranPlugin() {
        return dataTranPlugin;
    }

    /**
     * 记录作业处理过程中的异常日志
     *
     * @param taskContext
     * @param msg
     * @param e
     */
    @Override
    public void reportJobMetricErrorLog( TaskContext taskContext, String msg, Throwable e) {
        dataTranPlugin.reportJobMetricErrorLog(  taskContext, msg, e);
    }

    /**
     * 记录作业处理过程中的日志
     *
     * @param taskContext
     * @param msg
     */
    @Override
    public void reportJobMetricLog(TaskContext taskContext, String msg) {
        dataTranPlugin.reportJobMetricLog(taskContext, msg);
    }

    /**
     * 记录作业处理过程中的日志
     *
     * @param taskContext
     * @param msg
     */
    @Override
    public void reportJobMetricWarn( TaskContext taskContext, String msg) {
        dataTranPlugin.reportJobMetricWarn( taskContext, msg);
    }

    /**
     * 记录作业任务处理过程中的异常日志
     *
     * @param taskMetrics
     * @param msg
     * @param e
     */
    @Override
    public void reportTaskMetricErrorLog(TaskMetrics taskMetrics, String msg, Throwable e) {
        dataTranPlugin.reportTaskMetricErrorLog(taskMetrics, msg, e);
    }

    /**
     * 记录作业任务处理过程中的日志
     *
     * @param taskMetrics
     * @param msg
     */
    @Override
    public void reportTaskMetricLog(TaskMetrics taskMetrics, String msg) {
        dataTranPlugin.reportTaskMetricLog(taskMetrics, msg);
    }

    /**
     * 记录作业任务处理过程中的日志
     *
     * @param taskMetrics
     * @param msg
     */
    @Override
    public void reportTaskMetricWarn(TaskMetrics taskMetrics, String msg) {
        dataTranPlugin.reportTaskMetricWarn(taskMetrics, msg);
    }
}
