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

import org.frameworkset.tran.AsynTranResultSet;
import org.frameworkset.tran.Data;
import org.frameworkset.tran.context.ImportContext;

/**
 * <p>Description: 支持记录切割功能</p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/9/20 19:31
 * @author biaoping.yin
 * @version 1.0
 */
public class AsynSplitTranResultSet extends SplitTranResultSet implements AsynTranResultSet {
	private AsynTranResultSet tranResultSet;
	public AsynSplitTranResultSet(ImportContext importContext, AsynTranResultSet tranResultSet){
		super(  importContext,tranResultSet);
		this.tranResultSet = tranResultSet;
	}
	@Override
	public void appendData(Data datas) {
		tranResultSet.appendData(datas);
	}
}
