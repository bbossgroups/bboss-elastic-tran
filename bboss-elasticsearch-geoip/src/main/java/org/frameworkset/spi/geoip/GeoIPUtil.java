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


import com.fasterxml.jackson.databind.JsonNode;
import com.frameworkset.util.SimpleStringUtil;
import com.maxmind.db.CHMCache;
import com.maxmind.db.Reader;
import com.maxmind.db.Record;
import org.frameworkset.elasticsearch.ElasticSearchHelper;
import org.frameworkset.elasticsearch.entity.geo.GeoPoint;
import org.frameworkset.spi.ip2region.IP2Region;
import org.frameworkset.spi.remote.http.HttpRequestUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
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
	public GeoIPFilter getGeoIPFilter() {
		return geoIPFilter;
	}
	private boolean assertEmpty(){
		if(SimpleStringUtil.isEmpty(database) || SimpleStringUtil.isEmpty(asnDatabase))
			return true;
		return false;
	}
	public void init(){
		geoIPFilter = assertEmpty()?null:new GeoIPFilter(database,asnDatabase,cachesize);
		if(ip2regionDatabase != null && !ip2regionDatabase.equals("")){
			IP2Region ip2Region = new IP2Region();
			ip2Region.init(ip2regionDatabase);
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
	public static void main(String[] args) throws IOException {
		// 测试ip 221.232.245.73 湖北武汉
		try {
			GeoIPUtil addressUtils = new GeoIPUtil();
			addressUtils.setAsnDatabase("E:\\workspace\\hnai\\terminal\\geolite2\\GeoLite2-ASN.mmdb");
			addressUtils.setDatabase("E:\\workspace\\hnai\\terminal\\geolite2\\GeoLite2-City.mmdb");
			addressUtils.setIp2regionDatabase("E:\\workspace\\ip2region-master\\data\\ip2region.db");
			addressUtils.setCachesize(2000);
			addressUtils.init();
			addressUtils.setIpUrl("http://ip.taobao.com/service/getIpInfo.php");
         	IpInfo address = addressUtils.getAddressMapResult("183.15.204.103");
			System.out.println(address);
		} catch (Exception e) {
			 e.printStackTrace();
		}
		File database = new File("E:\\workspace\\hnai\\terminal\\geolite2\\GeoLite2-City.mmdb");
			Reader reader = new Reader(database,new CHMCache(4096));
			InetAddress address = InetAddress.getByName("183.15.204.103");

			// get() returns just the data for the associated record
			JsonNode recordData = reader.get(address);

			System.out.println(recordData);

			// getRecord() returns a Record class that contains both
			// the data for the record and associated metadata.
			Record record = reader.getRecord(address);

			System.out.println(record.getData());
			System.out.println(record.getNetwork());


		 database = new File("E:\\workspace\\hnai\\terminal\\geolite2\\GeoLite2-ASN.mmdb");
		 reader = new Reader(database,new CHMCache(4096));
		 address = InetAddress.getByName("183.15.204.103");

		// get() returns just the data for the associated record
		 recordData = reader.get(address);

		System.out.println(recordData);

		// getRecord() returns a Record class that contains both
		// the data for the record and associated metadata.
		 record = reader.getRecord(address);

		System.out.println(record.getData());
		System.out.println(record.getNetwork());
         // 输出结果为：中国 湖北省 武汉市

		database = new File("E:\\workspace\\hnai\\terminal\\geolite2\\GeoLite2-Country.mmdb");
		reader = new Reader(database,new CHMCache(4096));
		address = InetAddress.getByName("183.15.204.103");

		// get() returns just the data for the associated record
		recordData = reader.get(address);

		System.out.println(recordData);

		// getRecord() returns a Record class that contains both
		// the data for the record and associated metadata.
		record = reader.getRecord(address);

		System.out.println(record.getData());
		System.out.println(record.getNetwork());
	}

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

	public IpInfo getAddressMapResult(String ip)  {

//			ip = "117.158.148.162";
//			geoData_ = this.geoIPFilter.handleIp(ip);
		if(ip2Region != null){
			IpInfo ipInfo = ip2Region.getAddressMapResult(ip);
			if(ipInfo != null){
				return ipInfo;
			}
		}

//			ip = "240e:c0:f450:cb84:5dc4:928c:cd42:342b";
		//从geolite2获取ip地址信息
		Map<String,Object> geoData_ = geoIPFilter != null ?this.geoIPFilter.handleIp(ip):new HashMap<String, Object>();
//			Map<String,Object> taobaodata = HttpRequestUtil.httpGetforString(url.toString(),header,new MapResponseHandler());




		if(geoData_ != null && geoData_.size() > 0) {//处理从geolite2获取ip地址信息
			Map<String, Object> asnData_ = this.geoIPFilter.handleIpAsn(ip);
			IpInfo ipInfo = new IpInfo();
			ipInfo.setIp(ip);
			ipInfo.setRegionId((String)geoData_.get("regionCode"));
			ipInfo.setArea("");
			ipInfo.setAreaId("");
			ipInfo.setCity((String)geoData_.get("cityName"));
			ipInfo.setCityId("");
			ipInfo.setCountry((String)geoData_.get("countryName"));
			ipInfo.setCountryId((String)geoData_.get("countryCode2"));
			ipInfo.setCounty((String)geoData_.get("regionName"));
			ipInfo.setCountyId((String)geoData_.get("regionCode"));
			ipInfo.setIsp((String)asnData_.get("asOrg"));
			ipInfo.setIspId((Integer)asnData_.get("asn"));
			ipInfo.setRegion((String)geoData_.get("regionName"));
			ipInfo.setRegionId((String)geoData_.get("regionCode"));
			Double latitude = (Double) geoData_.get("latitude");
			Double longitude = (Double) geoData_.get("longitude");
			GeoPoint geoPoint = new GeoPoint();
			geoPoint.setLat(latitude);
			geoPoint.setLon(longitude);
			ipInfo.setGeoPoint(geoPoint);
			return ipInfo;
		}
		else{//如果没有
//			StringBuilder url = new StringBuilder();
//			url.append(ipUrl).append("?ip=").append(ip);
//			try {
//				Map header = new HashMap();
////		StringBuilder url = new StringBuilder();
//				header.put("Content-Type","text/html;charset=UTF-8");
//				header.put("User-Agent", "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36");
//				Map<String, Object> data = HttpRequestUtil.httpGetforString(url.toString(), header, new MapResponseHandler());
//				Integer code = (Integer) data.get("code");
//				if (code != null && code == 0) {
//					Map<String, Object> ipdata = (Map<String, Object>) data.get("data");
//					IpInfo ipInfo = new IpInfo();
//					ipInfo.setArea((String) ipdata.get("area"));
//					ipInfo.setAreaId((String) ipdata.get("area_id"));
//					ipInfo.setCity((String) ipdata.get("city"));
//					ipInfo.setCityId((String) ipdata.get("city_id"));
//					ipInfo.setCountry((String) ipdata.get("country"));
//					ipInfo.setCountryId((String) ipdata.get("country_Id"));
//					ipInfo.setCounty((String) ipdata.get("county"));
//					ipInfo.setCountyId((String) ipdata.get("county_id"));
//					ipInfo.setIp((String) ipdata.get("ip"));
//					ipInfo.setIsp((String) ipdata.get("isp"));
//					ipInfo.setIspId((Integer) ipdata.get("isp_id"));
//					ipInfo.setRegion((String) ipdata.get("region"));
//					ipInfo.setRegionId((String) ipdata.get("region_id"));
//					//					Float latitude = (Float) geoData_.get("latitude");
//					//					Float longitude = (Float) geoData_.get("longitude");
//					//					ipInfo.setLatitude(latitude);
//					//					ipInfo.setLongitude(longitude);
//					return ipInfo;
//				} else {
					IpInfo ipInfo = new IpInfo();
					ipInfo.setArea("");
					ipInfo.setAreaId("");
					ipInfo.setCity("未知");
					ipInfo.setCityId("未知");
					ipInfo.setCountry("未知");
					ipInfo.setCountryId("未知");
					ipInfo.setCounty("未知");
					ipInfo.setCountyId("未知");
					ipInfo.setIp(ip);
					ipInfo.setIsp("未知");
					ipInfo.setIspId(null);
					ipInfo.setRegion("未知");
					ipInfo.setRegionId("未知");
					return ipInfo;
//				}
//			}
//			 catch (Exception e) {
//				url.setLength(0);
//				url.append("获取运营商区域信息异常:").append(ipUrl).append("?ip=").append(ip)
//						.append(",User-Agent:Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36")
//						.append(",Content-Type:text/html;charset=UTF-8");
//				throw new GeoIPHandlerException("获取运营商区域信息异常:"+url.toString(),e);
//			}
		}
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


	private static Logger logger = LoggerFactory.getLogger(GeoIPUtil.class);
	private static boolean getGeoIPUtil ;
	public static GeoIPUtil getGeoIPUtil(Map<String, String> geoipConfig) {
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
					geoIPUtil.setDatabase(geoipConfig.get("ip.database"));
					geoIPUtil.setAsnDatabase(geoipConfig.get("ip.asnDatabase"));
					geoIPUtil.setIp2regionDatabase(geoipConfig.get("ip.ip2regionDatabase"));
					String _cachsize = geoipConfig.get("ip.cachesize");
					if (_cachsize != null) {
						try {
							geoIPUtil.setCachesize(Integer.parseInt(_cachsize));
						} catch (Exception e) {
							logger.info("getGeoIPUtil ip.cachesize must be a number:" + _cachsize, e);
						}
					}
					geoIPUtil.setIpUrl(geoipConfig.get("ip.serviceUrl"));
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
