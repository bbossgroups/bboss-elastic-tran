package org.frameworkset.tran;
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

import org.frameworkset.tran.config.ClientOptions;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.es.ESField;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 *
 * @author biaoping.yin
 * @version 1.0
 * @Date 2018/12/4 11:35
 */
public class DefaultEsIdGenerator implements EsIdGenerator {
	@Override
	public Object genId(Context context) throws Exception {
		ClientOptions clientOptions = context.getClientOptions();
		ESField esIdField = clientOptions != null?clientOptions.getIdField():null;
		if (esIdField != null) {
			if(!esIdField.isMeta())
				return context.getValue(esIdField.getField());
			else
				return context.getMetaValue(esIdField.getField());

		}
		return null;
	}
}
