package org.frameworkset.spi.geoip;
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


import com.frameworkset.util.SimpleStringUtil;
import org.frameworkset.elasticsearch.ElasticSearchHelper;
import org.frameworkset.spi.ip2region.IP2Region;
import org.frameworkset.spi.remote.http.HttpRequestUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * <p>Description: 通过淘宝api获取ip对应的相关信息</p>
 * <p>返回参数详解
 *
 * code
 * 状态码，正常为0，异常的时候为非0。
 * data
 * 查询到的结果。
 * country
 * 国家。
 * country_id
 * 国家代码。
 * area
 * 地区名称（华南、华北...）。
 * area_id
 * 地区编号。
 * region
 * 省名称。
 * region_id
 * 省编号。
 * city
 * 市名称。
 * city_id
 * 市编号。
 * county
 * 县名称。
 * county_id
 * 县编号。
 * isp
 * ISP服务商名称（电信/联通/铁通/移动...）。
 * isp_id
 * ISP服务商编号。
 * ip
 * 查询的IP地址。</p>
 * <p>Copyright (c) 2018</p>
 * date 2019/3/25 18:45
 * @author biaoping.yin
 * @version 1.0
 */
public class GeoIPUtil {
	private GeoIPFilter geoIPFilter;
	private IP2Region ip2Region;

	public Object getIspConverter() {
		return ispConverter;
	}

	public void setIspConverter(Object ispConverter) {
		this.ispConverter = ispConverter;
	}

	private Object ispConverter;
	private IPConverter _IPConverter;
	public GeoIPFilter getGeoIPFilter() {
		return geoIPFilter;
	}
	private boolean assertEmpty(){
		return SimpleStringUtil.isEmpty(database) || SimpleStringUtil.isEmpty(asnDatabase);
	}
	public void init(){

		if(!assertEmpty()){
			if(ispConverter != null){
				if(ispConverter instanceof IPConverter){
					_IPConverter = (IPConverter)ispConverter;
				}
				else {
					String cls = ((String)ispConverter).trim();
					if(!cls.equals("")) {
						try {
							Class<? extends IPConverter> clazz = (Class<? extends IPConverter>) Class.forName(cls);
							_IPConverter = clazz.newInstance();
						} catch (ClassNotFoundException e) {
							logger.warn(cls, e);
							_IPConverter = new DefaultIPConverter();
						} catch (IllegalAccessException e) {
							logger.warn(cls, e);
							_IPConverter = new DefaultIPConverter();
						} catch (InstantiationException e) {
							logger.warn(cls, e);
							_IPConverter = new DefaultIPConverter();
						}
					}
					else{
						_IPConverter = new DefaultIPConverter();
					}
				}
			}
			else{
				_IPConverter = new DefaultIPConverter();
			}
			geoIPFilter = new GeoIPFilter(database,asnDatabase,cachesize);
		}

		if(ip2regionDatabase != null && !ip2regionDatabase.equals("")){
			IP2Region ip2Region = new IP2Region();
			ip2Region.init(ip2regionDatabase,false);
			this.ip2Region = ip2Region;
		}
	}

	public void setGeoIPFilter(GeoIPFilter geoIPFilter) {
		this.geoIPFilter = geoIPFilter;
	}

	private  String database;
	private String ip2regionDatabase;

	public String getAsnDatabase() {
		return asnDatabase;
	}

	public void setAsnDatabase(String asnDatabase) {
		this.asnDatabase = asnDatabase;
	}

	private  String asnDatabase;
	private int cachesize;
	public GeoIPUtil(){

	}
	public String getIpUrl() {
		return ipUrl;
	}

	public void setIpUrl(String ipUrl) {
		this.ipUrl = ipUrl;
	}

	private String ipUrl;


	/**
	 *
	 * @param ip
	 * @return
	 * @deprecated see public IpInfo getIpInfo(String ip)
	 */
	@Deprecated
	public String getAddressResult(String ip)  {

		StringBuilder url = new StringBuilder();
		url.append(ipUrl).append("?ip=").append(ip);
		Map header = new HashMap();
		header.put("Content-Type","text/html;charset=UTF-8");
		header.put("User-Agent", "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36");
		try {
			return HttpRequestUtil.httpGetforString(url.toString(),header);
		} catch (Exception e) {
			url.setLength(0);
			url.append("获取运营商区域信息异常:").append(ipUrl).append("?ip=").append(ip)
					.append(",User-Agent:Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36")
					.append(",Content-Type:text/html;charset=UTF-8");
			throw new GeoIPHandlerException("获取运营商区域信息异常:"+url.toString(),e);
		}
	}

	/**
	 *
	 * @param ip
	 * @return
	 * @deprecated see public IpInfo getIpInfo(String ip)
	 */
	@Deprecated
	public IpInfo getAddressMapResult(String ip)  {

		IpInfo ipInfo = null;
		boolean needHandle = false;
		if(ip2Region != null){
			ipInfo = ip2Region.getAddressMapResult(ip);

		}
		if(ipInfo == null){
			ipInfo = new IpInfo();
			ipInfo.setIp(ip);
		}
		else{
			needHandle = true;
		}
		if(geoIPFilter != null ) {
			geoIPFilter.handleIpInfo( ipInfo);
			needHandle = true;
		}
		if (ipInfo.getIsp() == null || ipInfo.getIsp().equals("0"))
			ipInfo.setIsp("未知");
		if (ipInfo.getArea() != null && ipInfo.getArea().equals("0"))
			ipInfo.setArea(null);
		if (ipInfo.getCountry() == null || ipInfo.getCountry().equals("0"))
			ipInfo.setCountry("未知");
		if (ipInfo.getCounty() != null && ipInfo.getCounty().equals("0"))
			ipInfo.setCounty("未知");
		if (ipInfo.getRegion() == null || ipInfo.getRegion().equals("0"))
			ipInfo.setRegion("未知");
		if (ipInfo.getCity() == null || ipInfo.getCity().equals("0"))
			ipInfo.setCity("未知");
		if(needHandle)
			_IPConverter.convert(ipInfo);

		return ipInfo;
		/**
//			ip = "240e:c0:f450:cb84:5dc4:928c:cd42:342b";
		//从geolite2获取ip地址信息
		Map<String,Object> geoData_ = geoIPFilter != null ?this.geoIPFilter.handleIp(ip):new HashMap<String, Object>();
//			Map<String,Object> taobaodata = HttpRequestUtil.httpGetforString(url.toString(),header,new MapResponseHandler());


		boolean needHanldle = true;

		if(geoData_ != null && geoData_.size() > 0) {//处理从geolite2获取ip地址信息
			Map<String, Object> asnData_ = this.geoIPFilter.handleIpAsn(ip);


			if(ipInfo.getArea() != null && ipInfo.getArea().equals("0")) {
				ipInfo.setArea(null);
				ipInfo.setAreaId(null);
			}
			if(ipInfo.getCity() == null || ipInfo.getCity().equals("0")) {
				ipInfo.setCity((String) geoData_.get("cityName"));
				ipInfo.setCityId(null);
			}
			if(ipInfo.getCountry() == null || ipInfo.getCountry().equals("0")) {
				ipInfo.setCountry((String) geoData_.get("countryName"));

			}
			ipInfo.setCountryId((String) geoData_.get("countryCode2"));
			if(ipInfo.getIsp() == null || ipInfo.getIsp().equals("0")) {
				ipInfo.setIsp((String) asnData_.get("asOrg"));
				ipInfo.setOrinIsp((String) asnData_.get("orinIsp"));
				ipInfo.setIspId((Integer)asnData_.get("asn"));
			}
			if(ipInfo.getRegion() == null || ipInfo.getRegion().equals("0")) {
				ipInfo.setRegion((String) geoData_.get("regionName"));
				ipInfo.setRegionId((String) geoData_.get("regionCode"));
			}
			Double latitude = (Double) geoData_.get("latitude");
			Double longitude = (Double) geoData_.get("longitude");
			GeoPoint geoPoint = new GeoPoint();
			geoPoint.setLat(latitude);
			geoPoint.setLon(longitude);
			ipInfo.setGeoPoint(geoPoint);
//			_IPConverter.convert(ipInfo);
//			return ipInfo;
		}
		else if(ipInfo == null){//如果没有
			needHanldle = false;
			ipInfo = new IpInfo();
//			ipInfo.setArea("");
//			ipInfo.setAreaId("");
			ipInfo.setCity("未知");
			ipInfo.setCityId(null);
			ipInfo.setCountry("未知");
			ipInfo.setCountryId("未知");
			ipInfo.setCounty(null);
			ipInfo.setCountyId(null);
			ipInfo.setIp(ip);
			ipInfo.setIsp("未知");
			ipInfo.setIspId(null);
			ipInfo.setRegion("未知");
			ipInfo.setRegionId(null);
//				return ipInfo;

		}
		if(needHanldle) {
			if (ipInfo.getIsp() == null || ipInfo.getIsp().equals("0"))
				ipInfo.setIsp("未知");
			if (ipInfo.getArea() != null && ipInfo.getArea().equals("0"))
				ipInfo.setArea("未知");
			if (ipInfo.getCountry() == null || ipInfo.getCountry().equals("0"))
				ipInfo.setCountry("未知");
			if (ipInfo.getCounty() != null && ipInfo.getCounty().equals("0"))
				ipInfo.setCounty("未知");
			if (ipInfo.getRegion() == null || ipInfo.getRegion().equals("0"))
				ipInfo.setRegion("未知");
			if (ipInfo.getCity() == null || ipInfo.getCity().equals("0"))
				ipInfo.setCity("未知");
			_IPConverter.convert(ipInfo);
		}
		return ipInfo;
		 */

	}

	/**
	 * 根据IP地址获取IP信息
	 * @param ip
	 * @return
	 */
	public IpInfo getIpInfo(String ip)  {

		return getAddressMapResult(ip);
	}

	public String getDatabase() {
		return database;
	}

	public void setDatabase(String database) {
		this.database = database;
	}

	public int getCachesize() {
		return cachesize;
	}

	public void setCachesize(int cachesize) {
		this.cachesize = cachesize;
	}


	private static final Logger logger = LoggerFactory.getLogger(GeoIPUtil.class);
	private static boolean getGeoIPUtil ;
	public static GeoIPUtil getGeoIPUtil(Map<String, Object> geoipConfig) {
		if(getGeoIPUtil)
			return  geoIPUtil;
		synchronized (GeoIPUtil.class){
			if(getGeoIPUtil)
				return  geoIPUtil;
			getGeoIPUtil = true;
			if(geoIPUtil == null) {

				try {
					if(geoipConfig == null)
						geoipConfig = ElasticSearchHelper.getGeoipConfig();
					if(geoipConfig == null || geoipConfig.size() == 0){
						return null;
					}
					GeoIPUtil geoIPUtil = new GeoIPUtil();
					geoIPUtil.setDatabase((String)geoipConfig.get("ip.database"));
					geoIPUtil.setAsnDatabase((String)geoipConfig.get("ip.asnDatabase"));
					geoIPUtil.setIp2regionDatabase((String)geoipConfig.get("ip.ip2regionDatabase"));
					geoIPUtil.setIspConverter(geoipConfig.get("ip.ispConverter"));
					String _cachsize = (String)geoipConfig.get("ip.cachesize");
					if (_cachsize != null) {
						try {
							geoIPUtil.setCachesize(Integer.parseInt(_cachsize));
						} catch (Exception e) {
							logger.info("getGeoIPUtil ip.cachesize must be a number:" + _cachsize, e);
						}
					}
					geoIPUtil.setIpUrl((String)geoipConfig.get("ip.serviceUrl"));
					geoIPUtil.init();
					GeoIPUtil.geoIPUtil = geoIPUtil;
				} catch (Exception e) {
					logger.info("getGeoIPUtil failed:", e);
				}
			}

		}
		return geoIPUtil;
	}



	private static GeoIPUtil geoIPUtil;

	public String getIp2regionDatabase() {
		return ip2regionDatabase;
	}

	public void setIp2regionDatabase(String ip2regionDatabase) {
		this.ip2regionDatabase = ip2regionDatabase;
	}
}
