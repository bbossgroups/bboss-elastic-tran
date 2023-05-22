package org.frameworkset.tran.input.excel;

import org.frameworkset.tran.input.file.FileConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author yinbp
 * @description
 * @create 2022/2/12
 */
public class ExcelFileConfig extends FileConfig {
    private Logger logger = LoggerFactory.getLogger(ExcelFileConfig.class);

    public ExcelFileConfig(){
        super();

    }
    public int getSheet() {
        return sheet;
    }


    public ExcelFileConfig setSheet(int sheet) {
        this.sheet = sheet;
        return this;
    }


    private int sheet;
    @Override
    public ExcelFileConfig init(){
        super.init();
        this.setEnableInode(false);
        this.setCloseEOF(true);//已经结束的文件内容采集完毕后关闭文件对应的采集通道，后续不再监听对应文件的内容变化

        return this;
    }
    @Override
    public void buildMsg(StringBuilder stringBuilder){
//		super.buildMsg(stringBuilder);
        stringBuilder.append(",sheet:").append(sheet);
        appendFieldList( stringBuilder);

    }
}
