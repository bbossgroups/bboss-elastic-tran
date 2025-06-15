package org.frameworkset.tran.record;
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

import org.frameworkset.elasticsearch.entity.KeyMap;
import org.frameworkset.tran.Record;
import org.frameworkset.tran.schedule.TaskContext;

import java.util.List;

/**
 * <p>Description: 记录切割器，将字段值切换为多条记录</p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/9/21 12:06
 * @author biaoping.yin
 * @version 1.0
 */
public interface SplitHandler {

	/**
	 * 将记录中字段对应的值切割为多条记录值
	 * @param taskContext
	 * @param record
	 * @param fieldValue
	 * @return KeyMap为LinkedHashMap的子类，添加key字段，用于设置用于kafka之类的消息key，如果是往kafka推送数据，可以设置推送的key，
	 * {@code public class KeyMap<K,V> extends LinkedHashMap<K,V> {}
	 *
	 * 	  标识记录key信息
	 *
	 * 	private Object key;
	 *
		public Object getKey() {
	 * 		return key;
	 * 	}
	 *
		public void setKey(Object key) {
	 * 		this.key = key;
	 * 	}
     * }
	 */
	public List<KeyMap> splitField(TaskContext taskContext, Record record, Object fieldValue);
}
