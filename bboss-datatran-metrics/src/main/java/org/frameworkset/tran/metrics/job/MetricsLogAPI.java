package org.frameworkset.tran.metrics.job;
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

 

/**
 * <p>Description: </p>
 * <p></p>
 *
 * @author biaoping.yin
 * @Date 2024/10/15
 */
public interface MetricsLogAPI<J,T> {
    /**
     * 记录作业处理过程中的异常日志
     * @param msg
     * @param e
     */
    default public void reportJobMetricErrorLog(J logcontext, String msg, Throwable e){

    }



    /**
     * 记录作业处理过程中的日志
     * @param msg
     */
    default public void reportJobMetricLog(J logcontext,String msg){

    }

    /**
     * 记录作业处理过程中的告警日志
     * @param msg
     */
    default public void reportJobMetricWarn(J logcontext,String msg){

    }

    /**
     * 记录作业处理过程中的debug日志
     * @param msg
     */
    default public void reportJobMetricDebug(J logcontext,String msg){

    }


    /**
     * 记录作业任务处理过程中的异常日志
     * @param msg
     * @param e
     */
    default public void reportTaskMetricErrorLog(T logcontext,String msg, Throwable e){

    }



    /**
     * 记录作业任务处理过程中的日志
     * @param msg
     */
    default public void reportTaskMetricLog(T logcontext, String msg){

    }

    /**
     * 记录作业任务处理过程中的告警日志
     * @param msg
     */
    default public void reportTaskMetricWarn(T logcontext, String msg){

    }

    /**
     * 记录作业任务处理过程中的告警日志
     * @param msg
     */
    default public void reportTaskMetricDebug(T logcontext, String msg){

    }

    /**
     * 记录作业处理过程中的异常日志
     * @param msg
     * @param e
     */
    default public void reportJobMetricErrorLog( String msg, Throwable e){
        this.reportJobMetricErrorLog(null,msg,e);
    }



    /**
     * 记录作业处理过程中的日志
     * @param msg
     */
    default public void reportJobMetricLog(String msg){
        this.reportJobMetricLog(null,msg);
    }

    /**
     * 记录作业处理过程中的告警日志
     * @param msg
     */
    default public void reportJobMetricWarn(String msg){
        this.reportJobMetricWarn(null,msg);
    }

    /**
     * 记录作业处理过程中的debug日志
     * @param msg
     */
    default public void reportJobMetricDebug(String msg){
        this.reportJobMetricDebug(null,msg);
    }


}
