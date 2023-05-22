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

import java.util.ArrayList;
import java.util.List;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2022/8/23
 * @author biaoping.yin
 * @version 1.0
 */
public class PersistentDataHolder {
	private List<KeyMetric> persistentData ;
	public void init(){
		persistentData = new ArrayList<>();
	}
	public void addKeyMetric(KeyMetric keyMetric){
		persistentData.add(keyMetric);
	}

	public List<KeyMetric> getPersistentData() {
		return persistentData;
	}
	public int size(){
		return persistentData.size();
	}
	public void clear(){
		this.persistentData = null;
	}
}
