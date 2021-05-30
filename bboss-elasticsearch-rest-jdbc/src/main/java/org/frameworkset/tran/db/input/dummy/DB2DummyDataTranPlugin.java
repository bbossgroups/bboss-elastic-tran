package org.frameworkset.tran.db.input.dummy;
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

import org.frameworkset.tran.BaseDataTran;
import org.frameworkset.tran.DataTranPlugin;
import org.frameworkset.tran.TranResultSet;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.db.input.SQLBaseDataTranPlugin;
import org.frameworkset.tran.ouput.dummy.DummyOutPutDataTran;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/10/9 14:35
 * @author biaoping.yin
 * @version 1.0
 */
public class DB2DummyDataTranPlugin extends SQLBaseDataTranPlugin implements DataTranPlugin {

	public DB2DummyDataTranPlugin(ImportContext importContext, ImportContext targetImportContext){
		super(importContext, targetImportContext);


	}




	public BaseDataTran createBaseDataTran(TaskContext taskContext, TranResultSet tranResultSet, Status currentStatus){
		DummyOutPutDataTran dummyOutPutDataTran = new DummyOutPutDataTran(  taskContext,tranResultSet,importContext,   targetImportContext, currentStatus);
		dummyOutPutDataTran.init();
		return dummyOutPutDataTran;
	}






}
