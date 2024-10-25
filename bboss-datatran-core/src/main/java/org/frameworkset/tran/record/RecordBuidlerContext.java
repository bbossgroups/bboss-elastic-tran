package org.frameworkset.tran.record;
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

import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.metrics.BaseMetricsLogReport;
import org.frameworkset.tran.schedule.TaskContext;


/**
 * <p>Description: 从原始数据集中构建Record记录对象</p>
 * <p></p>
 *
 * @author biaoping.yin
 * @Date 2024/5/16
 */
public class RecordBuidlerContext<T> extends BaseMetricsLogReport {
    private TaskContext taskContext;
    private ImportContext importContext;
    private T resultSet;

    public TaskContext getTaskContext() {
        return taskContext;
    }

    public void setTaskContext(TaskContext taskContext) {
        this.taskContext = taskContext;
    }

    public ImportContext getImportContext() {
        return importContext;
    }

    public void setImportContext(ImportContext importContext) {
        this.importContext = importContext;
    }

    public T getResultSet() {
        return resultSet;
    }

    public void setResultSet(T resultSet) {
        this.resultSet = resultSet;
    }

    /**
     * 记录作业处理过程中的异常日志
     *
     * @param msg
     * @param e
     */
    public void reportJobMetricErrorLog(  String msg, Throwable e) {
        dataTranPlugin.reportJobMetricErrorLog(  taskContext, msg, e);
    }

    /**
     * 记录作业处理过程中的日志
     *
     * @param msg
     */
    public void reportJobMetricLog(  String msg) {
        dataTranPlugin.reportJobMetricLog(taskContext, msg);
    }

    /**
     * 记录作业处理过程中的警告日志
     *
     * @param msg
     */
    public void reportJobMetricWarn(  String msg) {
        dataTranPlugin.reportJobMetricWarn(taskContext, msg);
    }

    /**
     * 记录作业处理过程中的debug日志
     *
     * @param msg
     */
    public void reportJobMetricDebug(  String msg) {
        dataTranPlugin.reportJobMetricDebug(taskContext, msg);
    }
}
