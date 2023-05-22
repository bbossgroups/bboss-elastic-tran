package org.frameworkset.spi.geoip;
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

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2020/11/12 22:04
 * @author biaoping.yin
 * @version 1.0
 */
public interface IPConverter {

	/**
	 * 对运营商进行转义处理
	 * 对国家进行转义处理
	 * 对城市进行转义处理
	 * 对省份进行转义处理
	 * 对区域（华南、华北）进行转义处理
	 * @param ipInfo
	 * @return
	 */
	IpInfo convert(IpInfo ipInfo);


}
