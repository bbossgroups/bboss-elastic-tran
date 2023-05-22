package org.frameworkset.tran.metrics.job;
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

import org.frameworkset.tran.metrics.entity.KeyMetric;

import java.util.*;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2022/8/15
 * @author biaoping.yin
 * @version 1.0
 */
public class KeyMetricsContainer  {

	/**
	 * s0和s1区域大小
	 */
	private int segmentBoundSize = 100000;

	public void setSegmentBoundSize(int segmentBoundSize) {
		this.segmentBoundSize = segmentBoundSize;
	}

	/**
	 * 用来保存基于key各个指标的统计数据
	 *  Map<指标key,KeyMetric> metrics
	 */
	private Map<String, KeyMetric> keyMetric = new LinkedHashMap<>();
	public void scanPersistentMetrics(PersistentScanCallback persistentScanCallback){
		List<String> removed = new ArrayList<>();
		Iterator<Map.Entry<String, KeyMetric>> iterator = keyMetric.entrySet().iterator();
		while(iterator.hasNext()){
			Map.Entry<String, KeyMetric> entry = iterator.next();

			if(persistentScanCallback.scanTimeMetric2Persistent(entry.getValue())){
				removed.add(entry.getValue().getMetric());
			}
			else{ //由于数据的顺序性，后续无需再判断
				break;
			}

		}
		if(removed.size() > 0){
			removed.forEach(metricKey ->{
				keyMetric.remove(metricKey);
			});

		}
	}
	public KeyMetric getKeyMetric(String metricKey){
			return keyMetric.get(metricKey);
	}
	public boolean reachSegmentBoundSize(int size) {
		return size >= segmentBoundSize;
	}
	public boolean putKeyMetric(String metricKey,KeyMetric keyMetric){
			this.keyMetric.put(metricKey, keyMetric);
			if(reachSegmentBoundSize(this.keyMetric.size())){
				return false;
			}
			return true;
	}

	public boolean isEmpty(){

		return keyMetric.isEmpty();

	}

	public Map<String, KeyMetric> getKeyMetric() {
		return keyMetric;
	}

	public void clean(){
		keyMetric = new LinkedHashMap<>();
	}
}
