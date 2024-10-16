package org.frameworkset.tran.metrics.entity;
/**
 * Copyright 2020 bboss
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

import org.frameworkset.tran.metrics.job.MetricsLogAPI;

import java.util.Date;
import java.util.concurrent.locks.ReentrantLock;

/**
 * <p>Description: 基于key维度的指标维度计算</p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/5/6 10:14
 * @author biaoping.yin
 * @version 1.0
 */
public abstract class KeyMetric {
    protected MetricsLogAPI metricsLogAPI;
	protected String metric;

	protected Date dataTime;
	protected Date slotTime;
	public abstract void init(MapData firstData);
	public abstract  void incr(MapData data);
	protected ReentrantLock incrementLock = new ReentrantLock();
	public String getMetric() {
		return metric;
	}
	public void increment(MapData data){
		incrementLock.lock();
		try {

			incr(data);
		}
		finally {
			incrementLock.unlock();
		}
	}



	public void setMetric(String metric) {
		this.metric = metric;
	}



	public Date getDataTime() {
		return dataTime;
	}

	public void setDataTime(Date dataTime) {
		this.dataTime = dataTime;
	}

	public Date getSlotTime() {
		return slotTime;
	}

	public void setSlotTime(Date slotTime) {
		this.slotTime = slotTime;
	}

	/**
	 * 总次数
	 */
	protected long count;

	protected Object min;
	protected Object max;
	protected Object avg;
	protected long success;
	protected long failed;


	public Object getMin() {
		return min;
	}

	public void setMin(Object min) {
		this.min = min;
	}

	public Object getMax() {
		return max;
	}

	public void setMax(Object max) {
		this.max = max;
	}

	public Object getAvg() {
		return avg;
	}

	public void setAvg(Object avg) {
		this.avg = avg;
	}





	public long getCount() {
		return count;
	}

	public void setCount(long count) {
		this.count = count;
	}



	public long getSuccess() {
		return success;
	}

	public void setSuccess(long success) {
		this.success = success;
	}

	public long getFailed() {
		return failed;
	}

	public void setFailed(long failed) {
		this.failed = failed;
	}

    public void setMetricsLogAPI(MetricsLogAPI metricsLogAPI) {
        this.metricsLogAPI = metricsLogAPI;
    }

    public MetricsLogAPI getMetricsLogAPI() {
        return metricsLogAPI;
    }
}
