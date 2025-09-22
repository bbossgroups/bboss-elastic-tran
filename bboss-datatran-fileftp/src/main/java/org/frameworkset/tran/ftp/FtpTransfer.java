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

import org.apache.commons.net.ftp.*;
import org.apache.commons.net.io.CopyStreamEvent;
import org.apache.commons.net.io.CopyStreamListener;
import org.apache.commons.net.util.TrustManagerUtils;
import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.input.file.FileFilter;
import org.frameworkset.tran.input.file.SFTPFilterFileInfo;
import org.frameworkset.tran.jobflow.scan.JobFileFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 *
 * @author biaoping.yin
 * @version 1.0
 * @Date 2021/2/1 20:27
 */
public class FtpTransfer {
	private static Logger logger = LoggerFactory.getLogger(FtpTransfer.class);

	/**
	 * 上传文件
	 * @param fileFtpOupputContext
	 * @param filePath
	 * @param remoteFilePath
	 */
	public static void sendFile(final FtpContext fileFtpOupputContext, final String filePath,
								final String remoteFilePath) {


		handle(fileFtpOupputContext, new FTPAction() {

			@Override
			public void execute(FTPClient ftp) throws IOException {
				InputStream input = null;
				try {
					if (logger.isInfoEnabled())
						logger.info("Send file to ftp " + fileFtpOupputContext.getFtpIP() + ":" + fileFtpOupputContext.getFtpPort() + " ,filePath[" + filePath + "],remote dir[" + remoteFilePath + "] start......");
					long startTime = System.currentTimeMillis();
					input = new FileInputStream(filePath);

					ftp.storeFile(remoteFilePath, input);
					long endTime = System.currentTimeMillis();
					if (logger.isInfoEnabled())
						logger.info("Send file to ftp " + fileFtpOupputContext.getFtpIP() + ":" + fileFtpOupputContext.getFtpPort() + " ,filePath[" + filePath + "],remote dir[" + remoteFilePath + "] complete, elapsed times:"+(endTime - startTime)+"毫秒.");
				} catch (Exception e) {
					throw new DataImportException("Send file to ftp " + fileFtpOupputContext.getFtpIP() + ":" + fileFtpOupputContext.getFtpPort() + " failed:filePath[" + filePath + "],remote dir[" + remoteFilePath + "]", e);
				} finally {
					if (input != null) {
						input.close();
					}
				}
			}
		});
	}

	/**
	 * 上传文件
	 * @param fileFtpOupputContext
	 * @param file
	 * @param remoteFilePath
	 */
	public static void sendFile(final FtpContext fileFtpOupputContext, final File file,
								final String remoteFilePath) {


		handle(fileFtpOupputContext, new FTPAction() {

			@Override
			public void execute(FTPClient ftp) throws IOException {
				InputStream input = null;
				try {
					if (logger.isInfoEnabled())
						logger.info("Send file to ftp " + fileFtpOupputContext.getFtpIP() + ":" + fileFtpOupputContext.getFtpPort() + " ,filePath[" + file.getAbsolutePath() + "],remote dir[" + remoteFilePath + "] start......");
					long startTime = System.currentTimeMillis();
					input = new FileInputStream(file);

					ftp.storeFile(remoteFilePath, input);
					long endTime = System.currentTimeMillis();
					if (logger.isInfoEnabled())
						logger.info("Send file to ftp " + fileFtpOupputContext.getFtpIP() + ":" + fileFtpOupputContext.getFtpPort() + " ,filePath[" + file.getAbsolutePath() + "],remote dir[" + remoteFilePath + "] complete, elapsed times:"+(endTime - startTime)+"毫秒.");
				} catch (Exception e) {
					throw new DataImportException("Send file to ftp " + fileFtpOupputContext.getFtpIP() + ":" + fileFtpOupputContext.getFtpPort() + " failed:filePath[" + file.getAbsolutePath() + "],remote dir[" + remoteFilePath + "]", e);
				} finally {
					if (input != null) {
						input.close();
					}
				}
			}
		});
	}

	/**
	 * 列出文件
	 * @param fileFtpOupputContext
	 * @return
	 */
	public static List<FTPFile> ls(final FtpContext fileFtpOupputContext) {
		final List<FTPFile> files = new ArrayList<FTPFile>();
		final FileFilter fileFilter = fileFtpOupputContext.getFileFilter();
        final JobFileFilter jobFileFilter = fileFtpOupputContext.getJobFileFilter();
		handle(fileFtpOupputContext, new FTPAction() {

			@Override
			public void execute(FTPClient ftp) throws IOException {
				try {
					FTPFile[] ftpFiles = null;
					if(fileFilter != null){
                        ftpFiles = ftp.listFiles(fileFtpOupputContext.getRemoteFileDir(), new FTPFileFilter() {
                            @Override
                            public boolean accept(FTPFile file) {
                                return fileFilter.accept(new FTPFilterFileInfo(fileFtpOupputContext.getRemoteFileDir(),file),fileFtpOupputContext.getFileConfig());                               

                            }
                        });
						
					}
                    else if(jobFileFilter != null){
                        ftpFiles = ftp.listFiles(fileFtpOupputContext.getRemoteFileDir(), new FTPFileFilter() {
                            @Override
                            public boolean accept(FTPFile file) {
                                

                                    return jobFileFilter.accept(new FTPFilterFileInfo(fileFtpOupputContext.getRemoteFileDir(),file),fileFtpOupputContext.getJobFlowNodeExecuteContext());

                            }
                        });
                    }
					else{
                        ftpFiles = ftp.listFiles(fileFtpOupputContext.getRemoteFileDir());
					}

					files.addAll(Arrays.asList(ftpFiles));
				} catch (Exception e) {
					throw new DataImportException("Ls files from ftp " + fileFtpOupputContext.getFtpIP() + ":" + fileFtpOupputContext.getFtpPort() + " failed:remote dir[" + fileFtpOupputContext.getRemoteFileDir() + "]", e);
				}
			}
		});
		return files;
	}

	/**
	 * 列出文件
	 * @param fileFtpOupputContext
	 * @return
	 */
	public static List<FTPFile> ls(final String remoteDir,final FtpContext fileFtpOupputContext) {
		final List<FTPFile> files = new ArrayList<FTPFile>();
		final FileFilter fileFilter = fileFtpOupputContext.getFileFilter();
        final JobFileFilter jobFileFilter = fileFtpOupputContext.getJobFileFilter();
		handle(fileFtpOupputContext, new FTPAction() {

			@Override
			public void execute(FTPClient ftp) throws IOException {
				try {
					FTPFile[] ftpFiles = null;
					if(fileFilter != null){
                        ftpFiles = ftp.listFiles(remoteDir, new FTPFileFilter() {
                            @Override
                            public boolean accept(FTPFile file) {

                                    return fileFilter.accept(new FTPFilterFileInfo(remoteDir,file),fileFtpOupputContext.getFileConfig());

                            }
                        });
						
					}
                    else if(jobFileFilter != null){
                        ftpFiles = ftp.listFiles(remoteDir, new FTPFileFilter() {
                            @Override
                            public boolean accept(FTPFile file) {

                                    return jobFileFilter.accept(new FTPFilterFileInfo(remoteDir,file),fileFtpOupputContext.getJobFlowNodeExecuteContext());

                            }
                        });
                    }
					else{
                        ftpFiles = ftp.listFiles(remoteDir);
					}

					files.addAll(Arrays.asList(ftpFiles));
				} catch (Exception e) {
					throw new DataImportException("Ls files from ftp " + fileFtpOupputContext.getFtpIP() + ":" + fileFtpOupputContext.getFtpPort() + " failed:remote dir[" + remoteDir + "]", e);
				}
			}
		});
		return files;
	}

	/**
	 * 下载文件
	 * @param fileFtpOupputContext
	 * @param local
	 * @param remote
	 */
	public static void downloadFile(final FtpContext fileFtpOupputContext, final String local,
									final String remote) {


		handle(fileFtpOupputContext, new FTPAction() {

			@Override
			public void execute(FTPClient ftp) throws IOException {
				OutputStream output = null;
				try {
					if (logger.isInfoEnabled()) {
						logger.info("Download file from ftp " + fileFtpOupputContext.getFtpIP() + ":" + fileFtpOupputContext.getFtpPort() + " ,remote[" + remote + "],local [" + local + "] start ......");
					}
					long startTime = System.currentTimeMillis();
					output = new FileOutputStream(local);
					ftp.retrieveFile(remote, output);
					long endTime = System.currentTimeMillis();
					if (logger.isInfoEnabled())
						logger.info("Download file from ftp " + fileFtpOupputContext.getFtpIP() + ":" + fileFtpOupputContext.getFtpPort() + " ,remote[" + remote + "],local [" + local + "] complete, elapsed times:"+(endTime - startTime)+"毫秒.");
				} catch (Exception e) {
					throw new DataImportException("Download file from ftp " + fileFtpOupputContext.getFtpIP() + ":" + fileFtpOupputContext.getFtpPort() + " failed:remote[" + remote + "],local [" + local + "]", e);
				} finally {
					if (output != null) {
						output.close();
					}
				}
			}
		});
	}

	/**
	 * 删除文件
	 * @param fileFtpOupputContext
	 * @param remoteFile
	 */
	public static void deleteFile(final FtpContext fileFtpOupputContext,final String remoteFile) {


		handle(fileFtpOupputContext, new FTPAction() {

			@Override
			public void execute(FTPClient ftp) throws IOException {
				try {
					ftp.deleteFile(remoteFile);
					if(logger.isInfoEnabled())
						logger.info("Delete file "+remoteFile+" from ftp " + fileFtpOupputContext.getFtpIP()+":"+fileFtpOupputContext.getFtpPort() + " success.");
				} catch (Exception e) {
					throw new DataImportException("Delete file "+remoteFile+" from ftp " + fileFtpOupputContext.getFtpIP()+":"+fileFtpOupputContext.getFtpPort() + " success.", e);
				}
			}
		});
	}

	private static void handle(FtpContext fileFtpOupputContext, FTPAction ftpAction) {
		final FTPClient ftp;
		if (fileFtpOupputContext.getFtpProtocol() == null) {
			if (fileFtpOupputContext.getFtpProxyHost() != null) {
				if (logger.isDebugEnabled())
					logger.debug("Using HTTP proxy server: " + fileFtpOupputContext.getFtpProxyHost());
				ftp = new FTPHTTPClient(fileFtpOupputContext.getFtpProxyHost(), fileFtpOupputContext.getFtpProxyPort(), fileFtpOupputContext.getFtpProxyUser(), fileFtpOupputContext.getFtpProxyPassword());
			} else {
				ftp = new FTPClient();
			}
		} else {
			FTPSClient ftps;
			if (fileFtpOupputContext.getFtpProtocol().equals("true")) {
				ftps = new FTPSClient(true);
			} else if (fileFtpOupputContext.getFtpProtocol().equals("false")) {
				ftps = new FTPSClient(false);
			} else {
				final String prot[] = fileFtpOupputContext.getFtpProtocol().split(",");
				if (prot.length == 1) { // Just protocol
					ftps = new FTPSClient(fileFtpOupputContext.getFtpProtocol());
				} else { // protocol,true|false
					ftps = new FTPSClient(prot[0], Boolean.parseBoolean(prot[1]));
				}
			}
			ftp = ftps;
			if ("all".equals(fileFtpOupputContext.getFtpTrustmgr())) {
				ftps.setTrustManager(TrustManagerUtils.getAcceptAllTrustManager());
			} else if ("valid".equals(fileFtpOupputContext.getFtpTrustmgr())) {
				ftps.setTrustManager(TrustManagerUtils.getValidateServerCertificateTrustManager());
			} else if ("none".equals(fileFtpOupputContext.getFtpTrustmgr())) {
				ftps.setTrustManager(null);
			}
		}

		if (fileFtpOupputContext.printHash()) {
			ftp.setCopyStreamListener(createListener());
		}

		if (fileFtpOupputContext.getSocketTimeout() > 0) {
			ftp.setDataTimeout(Duration.ofMillis(fileFtpOupputContext.getSocketTimeout()));
		}
		if (fileFtpOupputContext.getKeepAliveTimeout() > 0) {
			ftp.setControlKeepAliveTimeout(Duration.ofMillis(fileFtpOupputContext.getKeepAliveTimeout()));
		}
		if (fileFtpOupputContext.getControlKeepAliveReplyTimeout() > 0) {
			ftp.setControlKeepAliveReplyTimeout(Duration.ofMillis(fileFtpOupputContext.getControlKeepAliveReplyTimeout()));
		}
		if (fileFtpOupputContext.getEncoding() != null) {
			ftp.setControlEncoding(fileFtpOupputContext.getEncoding());
		}

		// suppress login details
//		ftp.addProtocolCommandListener(new PrintCommandListener(new PrintWriter(), true));

		/**
		 final FTPClientConfig config;
		 if (fileFtpOupputContext.getFtpServerType() != null) {
		 config = new FTPClientConfig(fileFtpOupputContext.getFtpServerType());
		 } else {
		 config = new FTPClientConfig();
		 }
		 config.setUnparseableEntries(saveUnparseable);
		 if (defaultDateFormat != null) {
		 config.setDefaultDateFormatStr(defaultDateFormat);
		 }
		 if (recentDateFormat != null) {
		 config.setRecentDateFormatStr(recentDateFormat);
		 }
		 ftp.configure(config);
		 */
		try {
			int reply;
			if (fileFtpOupputContext.getFtpPort() > 0) {
				ftp.connect(fileFtpOupputContext.getFtpIP(), fileFtpOupputContext.getFtpPort());
			} else {
				ftp.connect(fileFtpOupputContext.getFtpIP());
			}
			if(logger.isDebugEnabled())
				logger.debug("Connected to " + fileFtpOupputContext.getFtpIP() + " on " + (fileFtpOupputContext.getFtpPort() > 0 ? fileFtpOupputContext.getFtpPort() : ftp.getDefaultPort()));

			// After connection attempt, you should check the reply code to verify
			// success.
			reply = ftp.getReplyCode();

			if (!FTPReply.isPositiveCompletion(reply)) {
				ftp.disconnect();
				logger.info("FTP server refused connection.");
				throw new DataImportException("FTP server refused connection.");
			}
		} catch (final IOException e) {
			if (ftp.isConnected()) {
				try {
					ftp.disconnect();
				} catch (final IOException f) {
					// do nothing
				}
			}
			logger.info("Could not connect to server.");
			throw new DataImportException("Could not connect to server.", e);
		}

		try {
			if (!ftp.login(fileFtpOupputContext.getFtpUser(), fileFtpOupputContext.getFtpPassword())) {
				ftp.logout();
				throw new DataImportException("Could not connect to server: ftp user[" + fileFtpOupputContext.getFtpUser() + "] , ftp password[" + fileFtpOupputContext.getFtpPassword() + "]");
			}

			if(fileFtpOupputContext.binaryTransfer() != null) {
				if (fileFtpOupputContext.binaryTransfer()) {
					ftp.setFileType(FTP.BINARY_FILE_TYPE);
				} else {
					// in theory this should not be necessary as servers should default to ASCII
					// but they don't all do so - see NET-500
					ftp.setFileType(FTP.ASCII_FILE_TYPE);
				}
			}

			// Use passive mode as default because most of us are
			// behind firewalls these days.
			if(fileFtpOupputContext.localActive() != null) {
				if (fileFtpOupputContext.localActive()) {
					ftp.enterLocalActiveMode();
				} else {
					ftp.enterLocalPassiveMode();
				}
			}
			if(fileFtpOupputContext.useEpsvWithIPv4() != null)
				ftp.setUseEPSVwithIPv4(fileFtpOupputContext.useEpsvWithIPv4());
//
//			InputStream input = null;
//			try   {
//				input = new FileInputStream(filePath);
//				ftp.storeFile(remoteFilePath, input);
//				if(logger.isInfoEnabled())
//					logger.info("Send file to ftp " + fileFtpOupputContext.getFtpIP()+":"+fileFtpOupputContext.getFtpPort() + " success:filePath["+filePath+"],remote dir["+remoteFilePath+"]");
//			}catch (Exception e){
//				throw new DataImportException("Send file to ftp " + fileFtpOupputContext.getFtpIP()+":"+fileFtpOupputContext.getFtpPort() + " failed:filePath["+filePath+"],remote dir["+remoteFilePath+"]",e);
//			}
//			finally {
//				if(input != null){
//					input.close();
//				}
//			}
			ftpAction.execute(ftp);

			if (fileFtpOupputContext.getKeepAliveTimeout() > 0) {
				showCslStats(ftp);
			}


			ftp.noop(); // check that control connection is working OK

			ftp.logout();
		} catch (final FTPConnectionClosedException e) {

			throw new DataImportException("Server closed connection.", e);
		} catch (final IOException e) {
			throw new DataImportException(e);
		} finally {
			if (ftp.isConnected()) {
				try {
					ftp.disconnect();
				} catch (final IOException f) {
					// do nothing
				}
			}
		}
	}

	private static void showCslStats(final FTPClient ftp) {
		@SuppressWarnings("deprecation") // debug code
		final int[] stats = ftp.getCslDebug();
		logger.info("CslDebug=" + Arrays.toString(stats));

	}

	private static CopyStreamListener createListener() {
		return new CopyStreamListener() {
			private long megsTotal = 0;

			@Override
			public void bytesTransferred(final CopyStreamEvent event) {
				bytesTransferred(event.getTotalBytesTransferred(), event.getBytesTransferred(), event.getStreamSize());
			}

			@Override
			public void bytesTransferred(final long totalBytesTransferred,
										 final int bytesTransferred, final long streamSize) {
				final long megs = totalBytesTransferred / 1000000;
				for (long l = megsTotal; l < megs; l++) {
					logger.info("#");
				}
				megsTotal = megs;
			}
		};
	}
}
