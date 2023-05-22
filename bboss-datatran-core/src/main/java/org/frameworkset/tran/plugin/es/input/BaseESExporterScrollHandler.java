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

import org.frameworkset.elasticsearch.entity.ESDatas;
import org.frameworkset.elasticsearch.scroll.HandlerInfo;
import org.frameworkset.elasticsearch.scroll.ParralBreakableScrollHandler;
import org.frameworkset.tran.context.ImportContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/1/11 15:19
 * @author biaoping.yin
 * @version 1.0
 */
public abstract class BaseESExporterScrollHandler<T> extends ParralBreakableScrollHandler<T> {
	protected ImportContext importContext ;

	protected final Logger logger = LoggerFactory.getLogger(this.getClass());
	public BaseESExporterScrollHandler(ImportContext importContext) {
		this.importContext = importContext;


	}

	public abstract void handle(ESDatas<T> response, HandlerInfo handlerInfo) throws Exception ;


}
