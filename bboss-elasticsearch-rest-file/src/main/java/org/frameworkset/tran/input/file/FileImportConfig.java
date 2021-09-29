package org.frameworkset.tran.input.file;

import org.frameworkset.tran.config.BaseImportConfig;
import org.frameworkset.tran.ftp.FtpConfig;

import java.util.ArrayList;
import java.util.List;

/**
 * @author xutengfei,yin-bp@163.com
 * @description
 * @create 2021/3/12
 */
public class FileImportConfig extends BaseImportConfig {
    /**
     * 扫描新文件和文件名调整时间间隔
     */

    private Long interval = 5000L;
    private Long registLiveTime;
    //jsondata = true时，自定义的数据是否和采集的数据平级，true则直接在原先的json串中存放数据
    //false则定义一个json存放数据，若不是json则是message
    private boolean rootLevel = true;
    /**
     * jsondata：标识文本记录是json格式的数据，true 将值解析为json对象，false - 不解析，这样值将作为一个完整的message字段存放到上报数据中
     */
    private boolean jsondata ;
    private boolean enableMeta;
    private String charsetEncode = "UTF-8";
    private List<FileConfig> fileConfigList;
    private long checkFileModifyInterval = 3000l;
    /**
     * ftp配置冻结标识
     */
    private boolean ftpFreezon ;
    private boolean fromFtp;

    public FileImportConfig() {
    }

    public boolean isFtpFreezon() {
        return ftpFreezon;
    }

    public boolean isFromFtp() {
        return fromFtp;
    }

    public List<FileConfig> getFileConfigList() {
        return fileConfigList;
    }
    public FileImportConfig addConfig(FileConfig fileConfig){
        if(ftpFreezon && !(fileConfig instanceof FtpConfig))
            throw new FilelogPluginException("只能设置ftp config配置");
        if(fileConfigList == null){
            fileConfigList = new ArrayList<FileConfig>();
        }
        fileConfig.init();
        fileConfigList.add(fileConfig);
        if(fileConfig instanceof FtpConfig){
            ftpFreezon = true;
            fromFtp = true;
        }
        return this;
    }
    public FileImportConfig addConfig(String sourcePath,String fileNameRegular,String fileHeadLine){
        if(fileConfigList == null){
            fileConfigList = new ArrayList<FileConfig>();
        }
        fileConfigList.add(new FileConfig(sourcePath,fileNameRegular,fileHeadLine).init());
        return this;
    }
    public FileImportConfig addConfig(String sourcePath,String fileNameRegular,String fileHeadLine,boolean scanChild){
        if(fileConfigList == null){
            fileConfigList = new ArrayList<FileConfig>();
        }
        fileConfigList.add(new FileConfig(sourcePath,fileNameRegular,fileHeadLine,scanChild).init());
        return this;
    }

    public FileImportConfig setInterval(Long interval) {
        this.interval = interval;
        return this;
    }

    public Long getInterval() {
        return interval;
    }

    public boolean isRootLevel() {
        return rootLevel;
    }

    public boolean isJsondata() {
        return jsondata;
    }

    public FileImportConfig setRootLevel(boolean rootLevel) {
        this.rootLevel = rootLevel;
        return this;
    }

    public FileImportConfig setJsondata(boolean jsondata) {
        this.jsondata = jsondata;
        return this;
    }

    public String getCharsetEncode() {
        return charsetEncode;
    }

    public FileImportConfig setCharsetEncode(String charsetEncode) {
        this.charsetEncode = charsetEncode;
        return this;
    }

    public boolean isEnableMeta() {
        return enableMeta;
    }

    public FileImportConfig setEnableMeta(boolean enableMeta) {
        this.enableMeta = enableMeta;
        return this;
    }

    public Long getRegistLiveTime() {
        return registLiveTime;
    }

    public FileImportConfig setRegistLiveTime(Long registLiveTime) {
        this.registLiveTime = registLiveTime;
        return this;
    }


    public long getCheckFileModifyInterval() {
        return checkFileModifyInterval;
    }

    public FileImportConfig setCheckFileModifyInterval(long checkFileModifyInterval) {
        this.checkFileModifyInterval = checkFileModifyInterval;
        return this;
    }

}
