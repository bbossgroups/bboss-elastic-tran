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
 * @Date 2020/11/13 13:04
 * @author biaoping.yin
 * @version 1.0
 */
public class IPUtil {
	public static void handleIsp(IpInfo ipInfo){
		String isp = ipInfo.getIsp();
		if(isp == null ){
			ipInfo.setIsp( "未知");
		}
		else if(isp.indexOf("Mobile") >= 0){
			ipInfo.setIsp( "移动");
		}
		else if(isp.indexOf("Chinanet") >= 0
				|| isp.indexOf("CHINATELECOM") >= 0
				|| isp.indexOf("Chinatelecom") >= 0
				|| isp.indexOf("Telecom") >= 0
				|| isp.indexOf("CHINANET") >= 0|| isp.indexOf("TELECOM") >= 0){
			ipInfo.setIsp( "电信");
		}
		else if(isp.indexOf("UNICOM") >= 0 || isp.indexOf("Unicom") >= 0 ){
			ipInfo.setIsp( "联通");
		}

		else if(isp.indexOf("TieTong") >= 0){
			ipInfo.setIsp( "铁通");
		}
		else if(isp.indexOf("Tencent") >= 0 ){
			ipInfo.setIsp( "腾讯");
		}
		else if(isp.indexOf("YAHOO") >= 0){
			ipInfo.setIsp( "雅虎");
		}
		else if(isp.indexOf("Huashu") >= 0){
			ipInfo.setIsp( "华数传媒");
		}
		else if(isp.indexOf("Huawei") >= 0){
			ipInfo.setIsp( "华为");
		}
		else if(isp.indexOf("CERNET2") >= 0){
			ipInfo.setIsp( "第二代中国教育和科研计算机网");
		}else if(isp.indexOf("Baidu") >= 0){
			ipInfo.setIsp( "百度");
		}
	}
	/**
	 * 处理地区信息
	 * @param ipInfo
	 */
	public static void handleIpInfo(IpInfo ipInfo){
		if(ipInfo == null){
			return ;
		}
		String city = ipInfo.getCity();
		if(city != null) {
			if (city.equals("响滩") || city.equals("湘潭")) {
				ipInfo.setCity("湘潭市");
			} else if (city.equals("塘渡口") || city.equals("邵阳")) {
				ipInfo.setCity("邵阳市");
			} else if (city.equals("永州")) {
				ipInfo.setCity("永州市");
			} else if (city.equals("郴州")) {
				ipInfo.setCity("郴州市");
			} else if (city.equals("株洲")) {
				ipInfo.setCity("株洲市");
			} else if (city.equals("常德")) {
				ipInfo.setCity("常德市");
			} else if (city.equals("沉阳市") || city.equals("沉阳")) {
				ipInfo.setCity("沈阳市");
			} else if (city.equals("长沙")) {
				ipInfo.setCity("长沙市");
				ipInfo.setRegion("湖南省");
			} else if (city.indexOf("湘西") >= 0 || city.equals("吉首")) {
				ipInfo.setCity("吉首市");
			} else if (city.equals("昆明")) {
				ipInfo.setCity("昆明市");
			} else if (city.equals("南昌")) {
				ipInfo.setCity("南昌市");
			} else if (city.equals("长春")) {
				ipInfo.setCity("长春市");
			} else if (city.equals("太原")) {
				ipInfo.setCity("太原市");
			}else if (city.equals("西安")) {
				ipInfo.setCity("西安市");
			}else if (city.equals("武汉")) {
				ipInfo.setCity("武汉市");
			}
			else if (city.equals("新北")) {
				ipInfo.setCity("新北市");
			}else if (city.equals("台北")) {
				ipInfo.setCity("台北市");
			}




		}

		String region = ipInfo.getRegion();
		if(region != null){
			if(region.equals("湖南")){
				ipInfo.setRegion("湖南省");
			}else if(region.equals("云南")){
				ipInfo.setRegion("云南省");
			}else if(region.equals("河南")){
				ipInfo.setRegion("河南省");
			}else if(region.equals("北京")){
				ipInfo.setRegion("北京市");
			}else if(region.equals("上海")){
				ipInfo.setRegion("上海市");
			}else if(region.equals("天津")){
				ipInfo.setRegion("天津市");
			}else if(region.equals("吉林")){
				ipInfo.setRegion("吉林省");
			}else if(region.equals("重庆")){
				ipInfo.setRegion("重庆市");
			}else if(region.equals("广东")){
				ipInfo.setRegion("广东省");
			}else if(region.equals("江西")){
				ipInfo.setRegion("江西省");
			}else if(region.equals("陕西")){
				ipInfo.setRegion("陕西省");
			}else if(region.equals("安徽")){
				ipInfo.setRegion("安徽省");
			}else if(region.equals("贵州")){
				ipInfo.setRegion("贵州省");
			}else if(region.equals("吉林")){
				ipInfo.setRegion("吉林省");
			}else if(region.equals("山西")){
				ipInfo.setRegion("山西省");
			}else if(region.equals("辽宁")){
				ipInfo.setRegion("辽宁省");
			}else if(region.equals("河北")){
				ipInfo.setRegion("河北省");
			}else if(region.equals("广西")){
				ipInfo.setRegion("广西壮族自治区");

			}else if(region.equals("海南")){
				ipInfo.setRegion("海南省");
			}else if(region.equals("甘肃")){
				ipInfo.setRegion("甘肃省");
			}else if(region.equals("香港")){

				ipInfo.setCity("香港");

			}else if(region.equals("香港市")){
				ipInfo.setRegion("香港");
				ipInfo.setCity("香港");

			}else if(region.equals("宁夏")){
				ipInfo.setRegion("宁夏回族自治区");

			}else if(region.equals("西藏")){
				ipInfo.setRegion("西藏自治区");

			}
			else if(region.equals("高雄市")){
				ipInfo.setRegion("台湾省");
				ipInfo.setCity("高雄市");
			}
			else if(region.equals("新北市")){
				ipInfo.setRegion("台湾省");
				ipInfo.setCity("新北市");
			}else if(region.equals("台湾")){
				ipInfo.setRegion("台湾省");
			}
		}
	}
}
