package org.frameworkset.tran.input.other;

import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.input.file.FileConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author yinbp
 * @description
 * @create 2022/2/12
 */
public class CommonFileConfig extends FileConfig {
    private Logger logger = LoggerFactory.getLogger(CommonFileConfig.class);

    private CommonFileExtractor commonFileExtractor;
    public CommonFileConfig(){
        super();

    }

    @Override
    public CommonFileConfig init(){
        super.init();
        this.setEnableInode(false);
        this.setCloseEOF(true);//已经结束的文件内容采集完毕后关闭文件对应的采集通道，后续不再监听对应文件的内容变化

        return this;
    }


    public CommonFileExtractor getCommonFileExtractor() {
        return commonFileExtractor;
    }

    public CommonFileConfig setCommonFileExtractor(CommonFileExtractor commonFileExtractor) {
        this.commonFileExtractor = commonFileExtractor;
        return this;
    }

    @Override
    public void build(){
        if(commonFileExtractor == null){
            throw new DataImportException("commonFileExtractor is null,please set use setCommonFileExtractor(CommonFileExtractor commonFileExtractor) . ");
        }
    }
}
