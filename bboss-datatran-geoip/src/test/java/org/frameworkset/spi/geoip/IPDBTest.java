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

import com.frameworkset.util.SimpleStringUtil;
import com.maxmind.db.CHMCache;
import com.maxmind.db.Reader;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.record.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Map;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2020/11/12 21:43
 * @author biaoping.yin
 * @version 1.0
 */
public class IPDBTest {
	private static Logger logger = LoggerFactory.getLogger(IPDBTest.class);
	public static void main(String[] args)  throws IOException{
		test() ;
	}
//	@Test
	public static void test() throws IOException {
				String fieldName = "a.keyword";
				System.out.println(fieldName.substring(0,fieldName.lastIndexOf(".keyword")));

			// 测试ip 221.232.245.73 湖北武汉
			try {
				GeoIPUtil addressUtils = new GeoIPUtil();
				addressUtils.setAsnDatabase("E:\\workspace\\hnai\\terminal\\geolite2\\GeoLite2-ASN.mmdb");
				addressUtils.setDatabase("E:\\workspace\\hnai\\terminal\\geolite2\\GeoLite2-City.mmdb");
				addressUtils.setIp2regionDatabase("E:\\workspace\\hnai\\terminal\\geolite2\\ip2region.db");
				addressUtils.setCachesize(2000);
				addressUtils.init();
//				addressUtils.setIpUrl("http://ip.taobao.com/service/getIpInfo.php");
				IpInfo address = addressUtils.getIpInfo("223.104.130.11");
				System.out.println(address);
				address = addressUtils.getIpInfo("2409:8950:5ee1:d5c4:a5ce:69f0:d9fb:72c8");
				System.out.println(address);
				DatabaseReader databaseReader = new DatabaseReader.Builder(new File("E:\\workspace\\hnai\\terminal\\geolite2\\GeoLite2-City.mmdb"))
																  .withCache(new CHMCache(2000)).build();
				final InetAddress ipAddress = InetAddress.getByName("2409:8950:5ee1:d5c4:a5ce:69f0:d9fb:72c8");
				CityResponse response = databaseReader.city(ipAddress);
				Country country = response.getCountry();
				City city = response.getCity();
				Location location = response.getLocation();
				Continent continent = response.getContinent();
				Postal postal = response.getPostal();
				Subdivision subdivision = response.getMostSpecificSubdivision();

				address = addressUtils.getIpInfo("185.180.222.151");
				System.out.println(address);
				address = addressUtils.getIpInfo("103.254.69.246");
				System.out.println(address);
				address = addressUtils.getIpInfo("180.168.192.126");
				System.out.println(address);
				address = addressUtils.getIpInfo("2408:84e2:1db:c9ec:4062:61d4:9340:85f7");
				System.out.println(address);
				address = addressUtils.getIpInfo("172.168.22.26");
				System.out.println(address);
				address = addressUtils.getIpInfo("36.148.49.213");
				System.out.println(address);
				address = addressUtils.getIpInfo("10.11.13.12");

				System.out.println(address);

			} catch (Exception e) {
				e.printStackTrace();
			}
			File database = new File("E:\\workspace\\hnai\\terminal\\geolite2\\GeoLite2-City.mmdb");
			Reader reader = new Reader(database,new CHMCache(4096));
			InetAddress address = InetAddress.getByName("36.148.49.213");


			// getRecord() returns a Record class that contains both
			// the data for the record and associated metadata.
			Map record = reader.get(address,Map.class);

			logger.info(SimpleStringUtil.object2json(record));



			database = new File("E:\\workspace\\hnai\\terminal\\geolite2\\GeoLite2-ASN.mmdb");
			reader = new Reader(database,new CHMCache(4096));
			address = InetAddress.getByName("183.15.204.103");

			// get() returns just the data for the associated record
		record = reader.get(address,Map.class);

		logger.info(SimpleStringUtil.object2json(record));

			// getRecord() returns a Record class that contains both
			// the data for the record and associated metadata.

			// 输出结果为：中国 湖北省 武汉市

			database = new File("E:\\workspace\\hnai\\terminal\\geolite2\\GeoLite2-Country.mmdb");
			reader = new Reader(database,new CHMCache(4096));
			address = InetAddress.getByName("183.15.204.103");

			// get() returns just the data for the associated record
		record = reader.get(address,Map.class);

		logger.info(SimpleStringUtil.object2json(record));
	}
}
