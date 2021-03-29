package org.frameworkset.tran.input.file;

import org.frameworkset.tran.config.BaseImportConfig;

import java.util.ArrayList;
import java.util.List;

/**
 * @author xutengfei
 * @description
 * @create 2021/3/12
 */
public class FileImportConfig extends BaseImportConfig {
    //监听间隔
    private Long interval;
    //jsondata = true时，自定义的数据是否和采集的数据平级，true则直接在原先的json串中存放数据
    //false则定义一个json存放数据，若不是json则是message
    private boolean rootLevel;
    /**
     * jsondata：标识文本记录是json格式的数据，true 将值解析为json对象，false - 不解析，这样值将作为一个完整的message字段存放到上报数据中
     */
    private boolean jsondata ;
    private String charsetEncode;
    private List<FileConfig> fileConfigList;

    public FileImportConfig(Long interval,boolean rootLevel) {
        this.interval = interval;
        this.rootLevel = rootLevel;
    }

    public FileImportConfig() {
        this.interval = 1000L;
    }
    public FileImportConfig(boolean rootLevel) {
        this();
        this.rootLevel = rootLevel;
    }

    public List<FileConfig> getFileConfigList() {
        return fileConfigList;
    }
    public void addConfig(FileConfig fileConfig){
        if(fileConfigList == null){
            fileConfigList = new ArrayList<FileConfig>();
        }
        fileConfigList.add(fileConfig);
    }
    public void addConfig(String sourcePath,String fileNameRegular,String fileHeadLine){
        if(fileConfigList == null){
            fileConfigList = new ArrayList<FileConfig>();
        }
        fileConfigList.add(new FileConfig(sourcePath,fileNameRegular,fileHeadLine));
    }
    public void addConfig(String sourcePath,String fileNameRegular,String fileHeadLine,boolean scanChild){
        if(fileConfigList == null){
            fileConfigList = new ArrayList<FileConfig>();
        }
        fileConfigList.add(new FileConfig(sourcePath,fileNameRegular,fileHeadLine,scanChild));
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

    public void setJsondata(boolean jsondata) {
        this.jsondata = jsondata;
    }

    public String getCharsetEncode() {
        return charsetEncode;
    }

    public void setCharsetEncode(String charsetEncode) {
        this.charsetEncode = charsetEncode;
    }
}
