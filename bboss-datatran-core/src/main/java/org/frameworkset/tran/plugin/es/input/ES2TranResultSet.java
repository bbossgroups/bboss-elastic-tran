package org.frameworkset.tran.plugin.es.input;
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

import org.frameworkset.tran.AsynBaseTranResultSet;
import org.frameworkset.tran.Record;
import org.frameworkset.tran.context.ImportContext;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/10/28 22:37
 * @author biaoping.yin
 * @version 1.0
 */
public class ES2TranResultSet extends AsynBaseTranResultSet {

	public ES2TranResultSet(ImportContext importContext) {
		super(importContext);

	}
	@Override
	protected Record buildRecord(Object data){
		return new ESRecord(getTaskContext(),this.importContext,data);
	}


}
