package org.frameworkset.tran.plugin.custom.input;
/**
 * Copyright 2026 bboss
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
import org.frameworkset.tran.schedule.TaskContext;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author biaoping.yin
 * @Date 2026/11/24
 */
public interface CustomDataGenerator {
	List<Map<String, Object>> generateData(GeneratorContext generatorContext);
	
	default Map<String, Object> getMetaDatas(Map<String, Object> data){
		return new LinkedHashMap<>();
	}
	/**
	 * 是否分页获取数据，如果是，分页起始位置和分页大小需要CustomDataGenerator自行维护
	 * @return
	 */
	default boolean isPaging(){
		return false;
	}
}
