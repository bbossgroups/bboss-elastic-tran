package org.frameworkset.tran.ftp;
/**
 * Copyright 2020 bboss
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.frameworkset.tran.input.RemoteContext;
import org.frameworkset.tran.input.file.FileConfig;
import org.frameworkset.tran.input.file.FileFilter;
import org.frameworkset.tran.input.file.FtpFileFilter;
import org.frameworkset.tran.input.file.RemoteFileChannel;
import org.frameworkset.tran.input.zipfile.ZipFilePasswordFunction;
import org.frameworkset.tran.jobflow.context.JobFlowNodeExecuteContext;
import org.frameworkset.tran.jobflow.scan.JobFileFilter;

import java.util.List;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/9/27 14:50
 * @author biaoping.yin
 * @version 1.0
 */
public class FtpContextImpl extends RemoteContext<FtpContextImpl> implements FtpContext {
	 private FtpConfig ftpConfig;

    protected JobFlowNodeExecuteContext jobFlowNodeExecuteContext;
    private JobFileFilter jobFileFilter;
	public FtpContextImpl(FtpConfig ftpConfig) {
		this.ftpConfig = ftpConfig;
	}
    public FtpContextImpl(FtpConfig ftpConfig,JobFlowNodeExecuteContext jobFlowNodeExecuteContext,JobFileFilter jobFileFilter) {
        this.ftpConfig = ftpConfig;
        this.jobFlowNodeExecuteContext = jobFlowNodeExecuteContext;
        this.jobFileFilter = jobFileFilter;
    }

    @Override
    public  Boolean enterLocalPassiveMode(){
        return ftpConfig.enterLocalPassiveMode();
    }

    /**
     * 工作流节点需要进行配置:设置本地文件存放路径
     *
     * @return
     */
    @Override
    public String getSourcePath() {
        return ftpConfig.getSourcePath();
    }

    @Override
    public String getUnzipDir() {
        return ftpConfig.getUnzipDir();
    }

    @Override
    public boolean isUnzip() {
        return ftpConfig.isUnzip();
    }

    @Override
    public String getZipFilePassward() {
        return ftpConfig.getZipFilePassward();
    }

    @Override
    public ZipFilePasswordFunction getZipFilePasswordFunction() {
        return ftpConfig.getZipFilePasswordFunction();
    }

    @Override
    public boolean isBackupSuccessFiles() {
        return ftpConfig.isBackupSuccessFiles();
    }

    @Override
    public boolean isTransferEmptyFiles() {
        return ftpConfig.isTransferEmptyFiles();
    }

    @Override
    public boolean isSendFileAsyn() {
        return ftpConfig.isSendFileAsyn();
    }

    @Override
    public int getSendFileAsynWorkThreads() {
        return ftpConfig.getSendFileAsynWorkThreads();
    }

    @Override
    public FtpConfig getFtpConfig() {
        return ftpConfig;
    }

    @Override
    public FileConfig getFileConfig() {
        return ftpConfig.getFileConfig();
    }

    @Override
    public JobFlowNodeExecuteContext getJobFlowNodeExecuteContext() {
        return jobFlowNodeExecuteContext;
    }

    @Override
	public String getFtpIP() {
		return ftpConfig.getFtpIP();
	}

 

	@Override
	public int getFtpPort() {
		return ftpConfig.getFtpPort();
	}

 
	@Override
	public String getRemoteFileDir() {
		return ftpConfig.getRemoteFileDir();
	}

	@Override
	public String getFtpUser() {
		return ftpConfig.getFtpUser();
	}

	@Override
	public String getFtpPassword() {
		return ftpConfig.getFtpPassword();
	}

	@Override
	public List<String> getHostKeyVerifiers() {
		return ftpConfig.getHostKeyVerifiers();
	}

	@Override
	public String getFtpProtocol() {
		return ftpConfig.getFtpProtocol();
	}

	@Override
	public String getFtpTrustmgr() {
		return ftpConfig.getFtpTrustmgr();
	}

	@Override
	public Boolean localActive() {
		return ftpConfig.isLocalActive();
	}

	@Override
	public Boolean useEpsvWithIPv4() {
		return ftpConfig.isUseEpsvWithIPv4();
	}

	@Override
	public int getTransferProtocol() {
		return ftpConfig.getTransferProtocol();
	}

	@Override
	public String getFtpProxyHost() {
		return ftpConfig.getFtpProxyHost();
	}

	@Override
	public int getFtpProxyPort() {
		return ftpConfig.getFtpProxyPort();
	}

	@Override
	public String getFtpProxyUser() {
		return ftpConfig.getFtpProxyUser();
	}

	@Override
	public String getFtpProxyPassword() {
		return ftpConfig.getFtpProxyPassword();
	}

	@Override
	public boolean printHash() {
		return ftpConfig.isPrintHash();
	}

	@Override
	public Boolean binaryTransfer() {
		return ftpConfig.isBinaryTransfer();
	}

	@Override
	public long getKeepAliveTimeout() {
		return ftpConfig.getKeepAliveTimeout();
	}

	@Override
	public long getSocketTimeout() {
		return ftpConfig.getSocketTimeout();
	}
	@Override
	public long getConnectTimeout() {
		return ftpConfig.getConnectTimeout();
	}

	@Override
	public int getControlKeepAliveReplyTimeout() {
		return ftpConfig.getControlKeepAliveReplyTimeout();
	}

	@Override
	public FileFilter getFileFilter() {     
        if(ftpConfig.getFileConfig() != null) {
            return ftpConfig.getFileConfig().getFileFilter();
        }
        return null;
	}

    @Override
    public JobFileFilter getJobFileFilter() {

        return this.jobFileFilter;
    }

	@Override
	public FtpFileFilter getFtpFileFilter() {
		return ftpConfig.getFtpFileFilter();
	}

	@Override
	public String getEncoding() {
		return ftpConfig.getEncoding();
	}


    public String getDownloadTempDir() {
        return ftpConfig.getDownloadTempDir();
    }

    @Override
    public RemoteFileChannel getRemoteFileChannel() {
        return ftpConfig.getRemoteFileChannel();
    }

    @Override
    public RemoteFileValidate getRemoteFileValidate() {
        return ftpConfig.getRemoteFileValidate();
    }

    @Override
    public void destroy() {
        ftpConfig.destroy();
    }

    /**
     * 单位：毫秒,默认1分钟
     *
     * @return
     */
    @Override
    public long getSuccessFilesCleanInterval() {
        return ftpConfig.getSuccessFilesCleanInterval();
    }

    @Override
    public long getFailedFileResendInterval() {
        return ftpConfig.getFailedFileResendInterval();
    }

    @Override
    public int getFileLiveTime() {
        return ftpConfig.getFileLiveTime();
    }
}
