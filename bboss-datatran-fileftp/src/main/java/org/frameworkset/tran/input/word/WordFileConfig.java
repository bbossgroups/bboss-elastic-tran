package org.frameworkset.tran.input.word;

import org.frameworkset.tran.input.file.FileConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author yinbp
 * @description
 * @create 2022/2/12
 */
public class WordFileConfig extends FileConfig {
    private Logger logger = LoggerFactory.getLogger(WordFileConfig.class);



    private WordExtractor wordExtractor;
    public WordFileConfig(){
        super();

    }

    @Override
    public WordFileConfig init(){
        super.init();
        this.setEnableInode(false);
        this.setCloseEOF(true);//已经结束的文件内容采集完毕后关闭文件对应的采集通道，后续不再监听对应文件的内容变化

        return this;
    }

    public WordExtractor getWordExtractor() {
        return wordExtractor;
    }

    public WordFileConfig setWordExtractor(WordExtractor wordExtractor) {
        this.wordExtractor = wordExtractor;
        return this;
    }
}
