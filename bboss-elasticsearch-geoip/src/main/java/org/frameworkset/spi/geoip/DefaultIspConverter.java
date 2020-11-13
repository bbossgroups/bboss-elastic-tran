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
 * @Date 2020/11/12 22:16
 * @author biaoping.yin
 * @version 1.0
 */
public class DefaultIspConverter implements IspConverter {
	@Override
	public String convert(String isp) {
		if(isp == null ){
			return "未知";
		}
		if(isp.indexOf("Mobile") >= 0){
			return "移动";
		}
		else if(isp.indexOf("Chinanet") >= 0
				|| isp.indexOf("CHINATELECOM") >= 0
				|| isp.indexOf("Chinatelecom") >= 0
				|| isp.indexOf("Telecom") >= 0
				|| isp.indexOf("CHINANET") >= 0){
			return "电信";
		}
		else if(isp.indexOf("UNICOM") >= 0 || isp.indexOf("Unicom") >= 0 ){
			return "联通";
		}

		else if(isp.indexOf("TieTong") >= 0){
			return "铁通";
		}
		else if(isp.indexOf("Tencent") >= 0 ){
			return "腾讯";
		}
		else if(isp.indexOf("YAHOO") >= 0){
			return "雅虎";
		}
		else if(isp.indexOf("Huashu") >= 0){
			return "华数传媒";
		}
		else if(isp.indexOf("Huawei") >= 0){
			return "华为";
		}
		else if(isp.indexOf("CERNET2") >= 0){
			return "第二代中国教育和科研计算机网";
		}else if(isp.indexOf("Baidu") >= 0){
			return "百度";
		}

		return isp;
	}
}
