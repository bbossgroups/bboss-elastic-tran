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

import com.fasterxml.jackson.databind.JsonNode;
import com.maxmind.db.CHMCache;
import com.maxmind.db.Reader;
import com.maxmind.db.Record;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2020/11/12 21:43
 * @author biaoping.yin
 * @version 1.0
 */
public class IPDBTest {
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
				addressUtils.setIpUrl("http://ip.taobao.com/service/getIpInfo.php");
				IpInfo address = addressUtils.getAddressMapResult("223.104.130.11");
				System.out.println(address);
				address = addressUtils.getAddressMapResult("2409:8950:5ee1:d5c4:a5ce:69f0:d9fb:72c8");
				System.out.println(address);

				address = addressUtils.getAddressMapResult("185.180.222.151");
				System.out.println(address);
				address = addressUtils.getAddressMapResult("103.254.69.246");
				System.out.println(address);
				address = addressUtils.getAddressMapResult("180.168.192.126");
				System.out.println(address);
				address = addressUtils.getAddressMapResult("2408:84e2:1db:c9ec:4062:61d4:9340:85f7");
				System.out.println(address);
				address = addressUtils.getAddressMapResult("172.168.22.26");
				System.out.println(address);
				address = addressUtils.getAddressMapResult("36.148.49.213");
				System.out.println(address);
				address = addressUtils.getAddressMapResult("10.11.13.12");

				System.out.println(address);

			} catch (Exception e) {
				e.printStackTrace();
			}
			File database = new File("E:\\workspace\\hnai\\terminal\\geolite2\\GeoLite2-City.mmdb");
			Reader reader = new Reader(database,new CHMCache(4096));
			InetAddress address = InetAddress.getByName("36.148.49.213");

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
}
