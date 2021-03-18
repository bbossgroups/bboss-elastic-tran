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
    private List<FileConfig> fileConfigList;

    public FileImportConfig(Long interval) {
        this.interval = interval;
    }

    public FileImportConfig() {
        this(1000L);
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
}
