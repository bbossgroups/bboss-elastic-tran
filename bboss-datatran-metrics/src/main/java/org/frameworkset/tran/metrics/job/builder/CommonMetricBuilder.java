package org.frameworkset.tran.metrics.job.builder;

import org.frameworkset.tran.metrics.entity.MapData;
import org.frameworkset.tran.metrics.entity.TimeMetric;
import org.frameworkset.tran.metrics.job.KeyMetricBuilder;

public class CommonMetricBuilder implements KeyMetricBuilder {
    private String key;
    protected String indexName;
    protected String indexType;
    protected String chnlCode;
    protected String browserType;
    protected String osType ;
    protected String isp ;
    protected String city;
    protected String region;
    @Override
    public boolean validateData(MapData data) {
        return true;
    }
    @Override
    public TimeMetric build() {
        return new TimeMetric() {
            /**
             * 指标属性初始化,必须初始化相关属性
             */
            public void init(MapData firstData) {
                // init with Map data
                this.metric = key;
            }

            /**
             * 指标计算：根据原始数据对相应的指标数据进行计数统计
             */
            @Override
            public void incr(MapData data) {
                //statics from Map data
                this.count++;
                this.ips++;
                this.pv++;
            }
        };
    }


    public String getBrowserType() {
        return browserType;
    }

    public void setBrowserType(String browserType) {
        this.browserType = browserType;
    }

    public String getOsType() {
        return osType;
    }

    public void setOsType(String osType) {
        this.osType = osType;
    }

    public String getIsp() {
        return isp;
    }

    public void setIsp(String isp) {
        this.isp = isp;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public String getChnlCode() {
        return chnlCode;
    }

    public void setChnlCode(String chnlCode) {
        this.chnlCode = chnlCode;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public String getIndexType() {
        return indexType;
    }

    public void setIndexType(String indexType) {
        this.indexType = indexType;
    }
}
