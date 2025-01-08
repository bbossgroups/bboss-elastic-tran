package org.frameworkset.tran.task;
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

import org.frameworkset.tran.CommonRecord;
import org.frameworkset.tran.context.Context;

import java.io.Writer;

/**
 * <p>Description: 数据同步并行处理流程指令</p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2022/3/14
 * @author biaoping.yin
 * @version 1.0
 */
public class BaseParrelTranCommand implements ParrelTranCommand{

	@Override
	public int hanBatchActionTask(TaskCommandContext taskCommandContext) {
		return 0;
	}
 

	@Override
	public void parrelCompleteAction() {

	}

	@Override
	public boolean splitCheck(long totalCount) {
		return false;
	}
}
