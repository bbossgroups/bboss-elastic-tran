package org.frameworkset.spi.geoip;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.frameworkset.elasticsearch.entity.geo.GeoPoint;

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
public class IpInfo implements java.io.Serializable{
//	//状态码，正常为0，异常的时候为非0。
//	private String code;

	// 国家。
	@JsonInclude(JsonInclude.Include.NON_NULL)
	private String country;
	// 国家代码。
	@JsonInclude(JsonInclude.Include.NON_NULL)
	private String countryId;
	// 地区名称（华南、华北...）。
	@JsonInclude(JsonInclude.Include.NON_NULL)
	private String area;
	// 地区编号。
	@JsonInclude(JsonInclude.Include.NON_NULL)
	private String areaId;
	// 省名称。
	@JsonInclude(JsonInclude.Include.NON_NULL)
	private String region;
	// 省编号。
	@JsonInclude(JsonInclude.Include.NON_NULL)
	private String regionId;
	// 市名称。
	@JsonInclude(JsonInclude.Include.NON_NULL)
	private String city;
	// 市编号。
	@JsonInclude(JsonInclude.Include.NON_NULL)
	private String cityId;
	// 县名称。
	@JsonInclude(JsonInclude.Include.NON_NULL)
	private String county;
	// 县编号。
	@JsonInclude(JsonInclude.Include.NON_NULL)
	private String countyId;
	// ISP服务商名称（电信/联通/铁通/移动...）。
	@JsonInclude(JsonInclude.Include.NON_NULL)
	private String isp;
	/**
	 * 根据ip获取的原始isp信息
	 */
	@JsonInclude(JsonInclude.Include.NON_NULL)
	private String orinIsp;
	// ISP服务商编号。
	@JsonInclude(JsonInclude.Include.NON_NULL)
	private Integer ispId;
	// 查询的IP地址
	@JsonInclude(JsonInclude.Include.NON_NULL)
	private String ip;
	/**
	 * 纬度

	 * 经度
	 */
	@JsonInclude(JsonInclude.Include.NON_NULL)
	private GeoPoint geoPoint;

	public String getCountry() {
		return country;
	}

	public void setCountry(String country) {
		this.country = country;
	}

	public String getCountryId() {
		return countryId;
	}

	public void setCountryId(String countryId) {
		this.countryId = countryId;
	}

	public String getArea() {
		return area;
	}

	public void setArea(String area) {
		this.area = area;
	}

	public String getAreaId() {
		return areaId;
	}

	public void setAreaId(String areaId) {
		this.areaId = areaId;
	}

	public String getRegion() {
		return region;
	}

	public void setRegion(String region) {
		this.region = region;
	}

	public String getRegionId() {
		return regionId;
	}

	public void setRegionId(String regionId) {
		this.regionId = regionId;
	}

	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}

	public String getCityId() {
		return cityId;
	}

	public void setCityId(String cityId) {
		this.cityId = cityId;
	}

	public String getCounty() {
		return county;
	}

	public void setCounty(String county) {
		this.county = county;
	}

	public String getCountyId() {
		return countyId;
	}

	public void setCountyId(String countyId) {
		this.countyId = countyId;
	}

	public String getIsp() {
		return isp;
	}

	public void setIsp(String isp) {
		this.isp = isp;
	}

	public Integer getIspId() {
		return ispId;
	}

	public void setIspId(Integer ispId) {
		this.ispId = ispId;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}



	public GeoPoint getGeoPoint() {
		return geoPoint;
	}

	public void setGeoPoint(GeoPoint geoPoint) {
		this.geoPoint = geoPoint;
	}

	public String getOrinIsp() {
		return orinIsp;
	}

	public void setOrinIsp(String orinIsp) {
		this.orinIsp = orinIsp;
	}
}
