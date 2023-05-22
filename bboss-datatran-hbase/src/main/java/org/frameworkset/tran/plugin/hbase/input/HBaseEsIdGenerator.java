package org.frameworkset.tran.plugin.hbase.input;
/**
 * Copyright 2008 biaoping.yin
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

import org.apache.hadoop.hbase.util.Bytes;
import org.frameworkset.tran.EsIdGenerator;
import org.frameworkset.tran.config.ClientOptions;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.plugin.es.ESField;

/**
 * <p>Description: 将hbase的id设置为es的文档_id</p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 *
 * @author biaoping.yin
 * @version 1.0
 * @Date 2018/12/4 11:35
 */
public class HBaseEsIdGenerator implements EsIdGenerator {
	@Override
	public Object genId(Context context) throws Exception {
		ClientOptions clientOptions = context.getClientOptions();
		ESField esIdField = clientOptions != null?clientOptions.getIdField():null;
		if (esIdField != null) {
			Object id = null;
			if(!esIdField.isMeta())
				id = context.getValue(esIdField.getField());
			else
				id = context.getMetaValue(esIdField.getField());
			if(id instanceof byte[])
				return Bytes.toString((byte[])id);
			else{
				return String.valueOf(id);
			}
		}
		return null;
	}
}
