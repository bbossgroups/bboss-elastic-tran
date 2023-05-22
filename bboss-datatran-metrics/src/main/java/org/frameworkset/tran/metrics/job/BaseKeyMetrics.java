package org.frameworkset.tran.metrics.job;
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Future;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2020/5/28 20:42
 * @author biaoping.yin
 * @version 1.0
 */
public abstract class BaseKeyMetrics {
	private static Logger logger = LoggerFactory.getLogger(BaseKeyMetrics.class);
	protected KeyMetricsPersistent metricsPersistent;

	public void setMetricsPersistent(KeyMetricsPersistent metricsPersistent) {
		this.metricsPersistent = metricsPersistent;
	}

	/**
	 * s0和s1区域大小
	 */
	protected int segmentBoundSize = 100000;
	/**
	 * 交换区s0
	 */
	protected KeyMetricsContainer keyMetricsContainerS0 ;
	/**
	 * 交换区s1
	 */
	protected KeyMetricsContainer keyMetricsContainerS1 ;

	public int getSegmentBoundSize() {
		return segmentBoundSize;
	}

	public void setSegmentBoundSize(int segmentBoundSize) {
		this.segmentBoundSize = segmentBoundSize;
	}


	protected KeyMetricsContainer buildKeyMetricsContainer(){
		KeyMetricsContainer keyMetricsContainer = new KeyMetricsContainer();
		keyMetricsContainer.setSegmentBoundSize(segmentBoundSize);
		return keyMetricsContainer;
	}

	public void init(){
		/**
		 * 交换区s0
		 */
		keyMetricsContainerS0 = buildKeyMetricsContainer();
		/**
		 * 交换区s1
		 */
		keyMetricsContainerS1 = buildKeyMetricsContainer();
		initMetrics();

	}

	protected abstract void initMetrics();


	protected Future persistent(KeyMetricsContainer keyMetricsContainer){
		return metricsPersistent.persistent( keyMetricsContainer.getKeyMetric().values());
	}


}
