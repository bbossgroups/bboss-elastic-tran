package org.frameworkset.tran.rocketmq.output;
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

import org.frameworkset.tran.DataImportException;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/2/26 16:23
 * @author biaoping.yin
 * @version 1.0
 */
public class RocketmqSendException extends DataImportException {
	public RocketmqSendException( Exception exception) {
		super(exception);
	}

	public RocketmqSendException(String message) {
		super(message);
	}

	public RocketmqSendException(String message, Throwable cause) {
		super(message, cause);
	}

	public RocketmqSendException(Throwable cause) {
		super(cause);
	}

	public RocketmqSendException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

 
}
