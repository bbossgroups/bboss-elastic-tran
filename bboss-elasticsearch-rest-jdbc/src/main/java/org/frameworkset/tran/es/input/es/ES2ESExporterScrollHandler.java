package org.frameworkset.tran.es.input.es;
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

import org.frameworkset.elasticsearch.entity.ESDatas;
import org.frameworkset.elasticsearch.scroll.HandlerInfo;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.es.BaseESExporterScrollHandler;
import org.frameworkset.tran.es.ESDatasWraper;
import org.frameworkset.tran.es.output.AsynESOutPutDataTran;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/1/11 15:19
 * @author biaoping.yin
 * @version 1.0
 */
public class ES2ESExporterScrollHandler<T>  extends BaseESExporterScrollHandler<T>{

	protected AsynESOutPutDataTran es2esDataTran ;
	public ES2ESExporterScrollHandler(ImportContext importContext, AsynESOutPutDataTran es2DBDataTran ) {
		super(  importContext);
		this.es2esDataTran = es2DBDataTran;
		this.es2esDataTran.setBreakableScrollHandler(this);
	}

	public void handle(ESDatas<T> response, HandlerInfo handlerInfo) throws Exception {//自己处理每次scroll的结果

//		ES2DBDataTran es2DBDataTran = new ES2DBDataTran(esTranResultSet,importContext);
		es2esDataTran.appendData(new ESDatasWraper(response));



	}


}
