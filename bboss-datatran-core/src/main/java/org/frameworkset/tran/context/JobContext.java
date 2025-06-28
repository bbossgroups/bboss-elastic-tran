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

import org.frameworkset.tran.jobflow.JobFlow;
import org.frameworkset.tran.jobflow.context.JobFlowExecuteContext;
import org.frameworkset.tran.jobflow.JobFlowNode;
import org.frameworkset.tran.jobflow.context.JobFlowNodeExecuteContext;
import org.frameworkset.tran.metrics.BaseMetricsLogReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.LinkedHashMap;
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
    private JobFlowNode jobFlowNode;
    private JobFlow jobFlow;

	public JobContext(){
		jobDatas = new LinkedHashMap<>();
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

    /**
     * 添加作业执行参数
     * @param name
     * @param value
     * @return
     */
    public JobContext addJobData(String name, Object value){
		jobDatas.put(name,value);
		return this;
	}

    /**
     * 将params中的每个元素作为单个参数添加为作业执行参数
     * @param params
     * @return
     */
    public JobContext addJobDatas(Map params){
        if(params != null){
            jobDatas.putAll(params);
        }
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
     * 记录作业处理过程中的告警日志
     *
     * @param msg
     */
    public void reportJobMetricWarn( String msg) {
        super.reportJobMetricWarn(null,msg);
    }
    /**
     * 记录作业处理过程中的debug日志
     *
     * @param msg
     */
    public void reportJobMetricDebug( String msg) {
        super.reportJobMetricDebug(null,msg);
    }

    public JobFlowNode getJobFlowNode() {
        return jobFlowNode;
    }

    public void setJobFlowNode(JobFlowNode jobFlowNode) {
        if (jobFlowNode != null){
            this.jobFlowNode = jobFlowNode;
            this.jobFlow = jobFlowNode.getJobFlow();
        }
    }

    public JobFlow getJobFlow() {
        return jobFlow;
    }

    public JobFlowExecuteContext getJobFlowExecuteContext() {
        if(jobFlowNode != null) {
            return jobFlowNode.getJobFlow().getJobFlowExecuteContext();
        }
        return null;
    }

    public JobFlowNodeExecuteContext getJobFlowNodeExecuteContext(){
        if(jobFlowNode != null){
            return jobFlowNode.getJobFlowNodeExecuteContext();
        }
        return null;
    }


 

 
    /**
     * 获取子节点所属的复合节点（串行/并行）执行上下文对象
     *
     * @return
     */
    public JobFlowNodeExecuteContext getContainerJobFlowNodeExecuteContext() {
        return getJobFlowNodeExecuteContext().getContainerJobFlowNodeExecuteContext();
    }

    public Object getJobFlowContextData(String name) {
        return this.getJobFlowExecuteContext().getContextData(name);
    }

    public Object getJobFlowContextData(String name, Object defaultValue) {
        return this.getJobFlowExecuteContext().getContextData(name,defaultValue);
    }

    public Object getJobFlowNodeContextData(String name) {
        return this.getJobFlowNodeExecuteContext().getContextData(name);
    }

    public Object getJobFlowNodeContextData(String name, Object defaultValue) {
        return this.getJobFlowNodeExecuteContext().getContextData(name,defaultValue);
    }

    public Object getContainerJobFlowNodeContextData(String name) {
        return this.getContainerJobFlowNodeExecuteContext().getContextData(name);
    }

    public Object getContainerJobFlowNodeContextData(String name, Object defaultValue) {
        return this.getContainerJobFlowNodeExecuteContext().getContextData(name,defaultValue);
    }

    public void addJobFlowNodeContextData(String name, Object data) {
        this.getJobFlowNodeExecuteContext().addContextData(name,data);
    }

    public void addJobFlowContextData(String name, Object data) {
        this.getJobFlowExecuteContext().addContextData(name,data);
    }

    public void addContainerJobFlowNodeContextData(String name, Object data) {
        this.getContainerJobFlowNodeExecuteContext().addContextData(name,data);
    }


    public String getJobFlowId() {
        return this.getJobFlowExecuteContext().getJobFlowId();
    }

    public String getJobFlowName() {
        return this.getJobFlowExecuteContext().getJobFlowName();
    }

    public String getJobFlowNodeId() {
        return this.getJobFlowNodeExecuteContext().getNodeId();
    }

    public String getJobFlowNodeName() {
        return this.getJobFlowNodeExecuteContext().getNodeName();
    }

    public String getContainerJobFlowNodeId() {
        return this.getContainerJobFlowNodeExecuteContext().getNodeId();
    }

    public String getContainerJobFlowNodeName() {
        return this.getContainerJobFlowNodeExecuteContext().getNodeName();
    }
}
