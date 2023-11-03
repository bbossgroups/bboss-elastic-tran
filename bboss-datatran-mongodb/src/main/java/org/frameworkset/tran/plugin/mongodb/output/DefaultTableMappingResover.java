package org.frameworkset.tran.plugin.mongodb.output;
/**
 * Copyright 2023 bboss
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

import org.frameworkset.tran.CommonRecord;
import org.frameworkset.tran.cdc.TableMapping;

/**
 * <p>Description:从记录元数据中提取表名称和数据库名称 </p>
 * <p></p>
 *
 * @author biaoping.yin
 * @Date 2023/11/3
 */
public class DefaultTableMappingResover implements TableMappingResover{
	@Override
	public TableMapping resover(CommonRecord commonRecord) {
		TableMapping tableMapping = new TableMapping();
		tableMapping.setKey((String)commonRecord.getMetaValue("table"));
		tableMapping.setTargetCollection((String)commonRecord.getMetaValue("table"));
		tableMapping.setTargetDatabase((String)commonRecord.getMetaValue("database"));
		return tableMapping;
	}
}
