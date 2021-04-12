package org.frameworkset.tran.input.file;

import org.frameworkset.tran.config.BaseImportConfig;
import org.frameworkset.tran.context.BaseImportContext;

import java.util.List;

/**
 * @author xutengfei,yin-bp@163.com
 * @description
 * @create 2021/3/12
 */
public class FileImportContext extends BaseImportContext {

    private FileImportConfig fileImportConfig;

    protected void init(BaseImportConfig baseImportConfig){
        super.init(baseImportConfig);
        fileImportConfig = (FileImportConfig)baseImportConfig;
    }
    public FileImportContext(){
        this(new FileImportConfig());

    }
    public long getCheckFileModifyInterval() {
        return fileImportConfig.getCheckFileModifyInterval();
    }
    public FileImportContext(BaseImportConfig baseImportConfig){
        super(baseImportConfig);
    }

    public FileImportConfig getFileImportConfig() {
        return fileImportConfig;
    }

    public void setFileImportConfig(FileImportConfig fileImportConfig) {
        this.fileImportConfig = fileImportConfig;
    }
    public boolean useFilePointer(){
        return true;
    }


    public List<FileConfig> getFileConfigList(){
        return fileImportConfig.getFileConfigList();
    }
}
