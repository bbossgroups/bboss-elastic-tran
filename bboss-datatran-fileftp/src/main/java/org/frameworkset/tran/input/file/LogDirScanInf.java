package org.frameworkset.tran.input.file;

import org.frameworkset.tran.schedule.TaskContext;

/**
 * 扫描新增日志文件
 * @author biaoping.yin
 * @description
 * @create 2021/3/23
 */
public interface LogDirScanInf {





    /**
     * 识别新增的文件，如果有新增文件，将启动新的文件采集作业
     */
    public void scanNewFile(TaskContext taskContext);


    /**
     * 判断是否是本地扫描还是远程扫描，ftp/sftp扫描时返回true
     * @return
     */
    public boolean isRemote() ;

    /**
     * 设置远程扫描和本地扫描
     * @param remote
     */
    public void setRemote(boolean remote) ;
}
