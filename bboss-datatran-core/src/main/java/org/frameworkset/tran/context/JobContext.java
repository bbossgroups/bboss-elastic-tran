package org.frameworkset.tran.context;
/**
 * Copyright 2022 bboss
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
import org.frameworkset.tran.metrics.BaseMetricsLogReport;
import org.frameworkset.tran.schedule.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2022/12/7
 * @author biaoping.yin
 * @version 1.0
 */
public class JobContext extends BaseMetricsLogReport {
	private static Logger logger = LoggerFactory.getLogger(JobContext.class);
	private Map<String,Object> jobDatas;
    private Date jobStartTime;
    private Date endStartTime;
	public JobContext(){
		jobDatas = new HashMap<String, Object>();
        jobStartTime = new Date();
	}

    public Date getJobStartTime() {
        return jobStartTime;
    }

    public void setEndStartTime(Date endStartTime) {
        this.endStartTime = endStartTime;
    }

    public Date getEndStartTime() {
        return endStartTime;
    }

    public JobContext addJobData(String name, Object value){
		jobDatas.put(name,value);
		return this;
	}
	public void release(){
		this.jobDatas.clear();
		this.jobDatas = null;
	}

	public Object getJobData(String name){
		return jobDatas.get(name);
	}

    /**
     * 记录作业处理过程中的异常日志
     *
     * @param msg
     * @param e
     */
    public void reportJobMetricErrorLog( String msg, Throwable e) {
        super.reportJobMetricErrorLog(  null, msg, e);
    }

    /**
     * 记录作业处理过程中的日志
     *
     * @param msg
     */
    public void reportJobMetricLog(  String msg) {
        super.reportJobMetricLog(null, msg);
    }

    /**
     * 记录作业处理过程中的日志
     *
     * @param msg
     */
    public void reportJobMetricWarn( String msg) {
        super.reportJobMetricWarn(null,msg);
    }

}
