package org.frameworkset.tran.input.csv;

import org.frameworkset.tran.input.file.FileConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author yinbp
 * @description
 * @create 2022/2/12
 */
public class CSVFileConfig extends FileConfig<CSVFileConfig> {
    private Logger logger = LoggerFactory.getLogger(CSVFileConfig.class);

    public CSVFileConfig(){
        super();

    }
    


    
    @Override
    public CSVFileConfig init(){
        super.init();
        this.setEnableInode(false);
        this.setCloseEOF(true);//已经结束的文件内容采集完毕后关闭文件对应的采集通道，后续不再监听对应文件的内容变化

        return this;
    }
    @Override
    public void buildMsg(StringBuilder stringBuilder){
//		super.buildMsg(stringBuilder);
        stringBuilder.append(",csv:");
        appendFieldList( stringBuilder);

    }
}
