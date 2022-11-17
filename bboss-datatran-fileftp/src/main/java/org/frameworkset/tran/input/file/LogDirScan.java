package org.frameworkset.tran.input.file;

import com.frameworkset.util.SimpleStringUtil;
import org.frameworkset.tran.plugin.file.input.FileInputDataTranPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.List;

/**
 * 扫描新增日志文件
 * @author biaoping.yin
 * @description
 * @create 2021/3/23
 */
public class LogDirScan implements LogDirScanInf{
    private Logger logger = LoggerFactory.getLogger(LogDirScan.class);
    protected FileConfig fileConfig;
    protected LogDirsScanThread logDirsScanThread;
    protected FileListenerService fileListenerService;
    private boolean remote;
    private FileInputDataTranPlugin fileInputDataTranPlugin;


    /**
     * Constructs a monitor with the specified interval and set of observers.
     *
     * @param fileConfig
     * @param fileListenerService .
     */
    public LogDirScan(LogDirsScanThread logDirsScanThread, FileConfig fileConfig, FileListenerService fileListenerService) {
        this.fileConfig = fileConfig;
        this.fileListenerService = fileListenerService;
        this.logDirsScanThread = logDirsScanThread;
        this.fileInputDataTranPlugin = fileListenerService.getFileInputDataTranPlugin();
    }



    public FileConfig getFileConfig() {
        return fileConfig;
    }



    /**
     * 识别新增的文件，如果有新增文件，将启动新的文件采集作业
     */
    @Override
    public void scanNewFile(){

        if(logger.isDebugEnabled()){
            logger.debug("scan new log file in dir {} with filename regex {}.",fileConfig.getLogDir(),fileConfig.getFileNameRegular());
        }
        File logDir = fileConfig.getLogDir();
        FilenameFilter filter = fileConfig.getFilter();
        List<File> completeFiles = new ArrayList<>();
        if(logDir.isDirectory() && logDir.exists()){
            File[] files = logDir.listFiles(filter);
            File file = null;
            for(int i = 0; files != null && i < files.length; i ++){
                if (!logDirsScanThread.isRunning()) {
                    break;
                }
                file = files[i];

                if(file.isFile() && file.exists()) {
                    int checkResult = fileListenerService.checkNewFile("",file,fileConfig);
                    fileListenerService.getFileInputDataTranPlugin().handleCompleteFiles(checkResult,file);
                }
                else if (fileConfig.isScanChild()){ //如果需要扫描子目录
                    scanSubDirNewFile(file.getName(),file);
                }
            }
        }
        else{
            logger.info("{} must be a directory or must be exists.",fileConfig.getSourcePath() );
        }
    }

    public void scanSubDirNewFile(String relativeParent,File logDir){
        if(logger.isDebugEnabled()){
            logger.debug("scan new log file in sub dir {}",logDir.getAbsolutePath());
        }
        FilenameFilter filter = fileConfig.getFilter();
        if(logDir.isDirectory() && logDir.exists()){
            File[] files = logDir.listFiles(filter);
            File file = null;
            for(int i = 0; files != null && i < files.length; i ++){
                if (!logDirsScanThread.isRunning()) {
                    break;
                }
                file = files[i];
                if(file.isFile() && file.exists()) {
                    int checkResult = fileListenerService.checkNewFile(relativeParent,file,fileConfig);
                    fileListenerService.getFileInputDataTranPlugin().handleCompleteFiles(checkResult,file);
                }
                else if (fileConfig.isScanChild()){ //如果需要扫描子目录
                    scanSubDirNewFile(SimpleStringUtil.getPath(relativeParent,file.getName()),file);
                }
            }
        }
        else{
            logger.info("{} must be a directory or must be exists.",logDir.getAbsolutePath());
        }
    }

    public boolean isRemote() {
        return remote;
    }

    public void setRemote(boolean remote) {
        this.remote = remote;
    }
}
