package org.frameworkset.tran.plugin.mysqlbinlog.input;
/**
 * Copyright 2025 bboss
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

 
import com.github.shyiko.mysql.binlog.BinaryLogDatabaseVersion;
import com.github.shyiko.mysql.binlog.GtidSet;
import com.github.shyiko.mysql.binlog.MariadbGtidSet;
import com.github.shyiko.mysql.binlog.event.*;
import com.github.shyiko.mysql.binlog.event.deserialization.*;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;
import com.github.shyiko.mysql.binlog.jmx.BinaryLogClientMXBean;
import com.github.shyiko.mysql.binlog.network.*;
import com.github.shyiko.mysql.binlog.network.protocol.*;
import com.github.shyiko.mysql.binlog.network.protocol.command.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.security.GeneralSecurityException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author biaoping.yin
 * Date 2025/8/27
 */
public class BinaryLogClientExt implements BinaryLogClientMXBean {

    private static final SSLSocketFactory DEFAULT_REQUIRED_SSL_MODE_SOCKET_FACTORY = new DefaultSSLSocketFactory() {

        @Override
        protected void initSSLContext(SSLContext sc) throws GeneralSecurityException {
            sc.init(null, new TrustManager[]{
                    new X509TrustManager() {

                        @Override
                        public void checkClientTrusted(X509Certificate[] x509Certificates, String s)
                                throws CertificateException { }

                        @Override
                        public void checkServerTrusted(X509Certificate[] x509Certificates, String s)
                                throws CertificateException { }

                        @Override
                        public X509Certificate[] getAcceptedIssuers() {
                            return new X509Certificate[0];
                        }
                    }
            }, null);
        }
    };
    private static final SSLSocketFactory DEFAULT_VERIFY_CA_SSL_MODE_SOCKET_FACTORY = new DefaultSSLSocketFactory();

    // https://dev.mysql.com/doc/internals/en/sending-more-than-16mbyte.html
    private static final int MAX_PACKET_LENGTH = 16777215;

    private final Logger logger = LoggerFactory.getLogger(getClass().getName());

    private final String hostname;
    private final int port;
    private final String schema;
    private final String username;
    private final String password;

    private boolean blocking = true;
    private long serverId = 65535;
    private volatile String binlogFilename;
    private volatile long binlogPosition = 4;
    private volatile long connectionId;
    private SSLMode sslMode = SSLMode.DISABLED;
    private boolean useNonGracefulDisconnect = false;

    protected GtidSet gtidSet;
    protected final Object gtidSetAccessLock = new Object();
    private boolean gtidSetFallbackToPurged;
    private boolean gtidEnabled = false;
    private boolean useBinlogFilenamePositionInGtidMode;
    protected Object gtid;
    private boolean tx;

    private EventDeserializer eventDeserializer = new EventDeserializer();

    private final List<BinaryLogClientExt.EventListener> eventListeners = new CopyOnWriteArrayList<BinaryLogClientExt.EventListener>();
    private final List<BinaryLogClientExt.LifecycleListener> lifecycleListeners = new CopyOnWriteArrayList<BinaryLogClientExt.LifecycleListener>();

    private SocketFactory socketFactory;
    private SSLSocketFactory sslSocketFactory;

    protected volatile PacketChannel channel;
    private volatile boolean connected;
    private volatile long masterServerId = -1;

    private ThreadFactory threadFactory;

    private boolean keepAlive = true;
    private long keepAliveInterval = TimeUnit.MINUTES.toMillis(1);

    private long heartbeatInterval;
    private volatile long eventLastSeen;

    private long connectTimeout = TimeUnit.SECONDS.toMillis(3);

    private volatile ExecutorService keepAliveThreadExecutor;

    private final Lock connectLock = new ReentrantLock();
    private final Lock keepAliveThreadExecutorLock = new ReentrantLock();
    private boolean useSendAnnotateRowsEvent;

    private BinaryLogDatabaseVersion databaseVersion;
    private int mariaDbSlaveCapability = 4;

    /**
     * Alias for BinaryLogClientExt("localhost", 3306, &lt;no schema&gt; = null, username, password).
     * @see BinaryLogClientExt#BinaryLogClientExt(String, int, String, String, String)
     * @param username login name
     * @param password password
     */
    public BinaryLogClientExt(String username, String password) {
        this("localhost", 3306, null, username, password);
    }

    /**
     * Alias for BinaryLogClientExt("localhost", 3306, schema, username, password).
     * @see BinaryLogClientExt#BinaryLogClientExt(String, int, String, String, String)
     * @param schema database name, nullable
     * @param username login name
     * @param password password
     */
    public BinaryLogClientExt(String schema, String username, String password) {
        this("localhost", 3306, schema, username, password);
    }

    /**
     * Alias for BinaryLogClientExt(hostname, port, &lt;no schema&gt; = null, username, password).
     * @see BinaryLogClientExt#BinaryLogClientExt(String, int, String, String, String)
     * @param hostname mysql server hostname
     * @param port mysql server port
     * @param username login name
     * @param password password
     */
    public BinaryLogClientExt(String hostname, int port, String username, String password) {
        this(hostname, port, null, username, password);
    }

    /**
     * @param hostname mysql server hostname
     * @param port mysql server port
     * @param schema database name, nullable. Note that this parameter has nothing to do with event filtering. It's
     * used only during the authentication.
     * @param username login name
     * @param password password
     */
    public BinaryLogClientExt(String hostname, int port, String schema, String username, String password) {
        this.hostname = hostname;
        this.port = port;
        this.schema = schema;
        this.username = username;
        this.password = password;
    }

    public boolean isBlocking() {
        return blocking;
    }

    /**
     * @param blocking blocking mode. If set to false - BinaryLogClientExt will disconnect after the last event.
     */
    public void setBlocking(boolean blocking) {
        this.blocking = blocking;
    }

    public SSLMode getSSLMode() {
        return sslMode;
    }

    public void setSSLMode(SSLMode sslMode) {
        if (sslMode == null) {
            throw new IllegalArgumentException("SSL mode cannot be NULL");
        }
        this.sslMode = sslMode;
    }

    public void setUseNonGracefulDisconnect(boolean useNonGracefulDisconnect) {
        this.useNonGracefulDisconnect = useNonGracefulDisconnect;
    }

    public long getMasterServerId() {
        return this.masterServerId;
    }

    /**
     * @return server id (65535 by default)
     * @see #setServerId(long)
     */
    public long getServerId() {
        return serverId;
    }

    /**
     * @param serverId server id (in the range from 1 to 2^32 - 1). This value MUST be unique across whole replication
     * group (that is, different from any other server id being used by any master or slave). Keep in mind that each
     * binary log client (mysql-binlog-connector-java/BinaryLogClientExt, mysqlbinlog, etc) should be treated as a
     * simplified slave and thus MUST also use a different server id.
     * @see #getServerId()
     */
    public void setServerId(long serverId) {
        this.serverId = serverId;
    }

    /**
     * @return binary log filename, nullable (and null be default). Note that this value is automatically tracked by
     * the client and thus is subject to change (in response to {@link EventType#ROTATE}, for example).
     * @see #setBinlogFilename(String)
     */
    public String getBinlogFilename() {
        return binlogFilename;
    }

    /**
     * @param binlogFilename binary log filename.
     * Special values are:
     * <ul>
     *   <li>null, which turns on automatic resolution (resulting in the last known binlog and position). This is what
     * happens by default when you don't specify binary log filename explicitly.</li>
     *   <li>"" (empty string), which instructs server to stream events starting from the oldest known binlog.</li>
     * </ul>
     * @see #getBinlogFilename()
     */
    public void setBinlogFilename(String binlogFilename) {
        this.binlogFilename = binlogFilename;
    }

    /**
     * @return binary log position of the next event, 4 by default (which is a position of first event). Note that this
     * value changes with each incoming event.
     * @see #setBinlogPosition(long)
     */
    public long getBinlogPosition() {
        return binlogPosition;
    }

    /**
     * @param binlogPosition binary log position. Any value less than 4 gets automatically adjusted to 4 on connect.
     * @see #getBinlogPosition()
     */
    public void setBinlogPosition(long binlogPosition) {
        this.binlogPosition = binlogPosition;
    }

    /**
     * @return thread id
     */
    public long getConnectionId() {
        return connectionId;
    }

    /**
     * @return GTID set. Note that this value changes with each received GTID event (provided client is in GTID mode).
     * @see #setGtidSet(String)
     */
    public String getGtidSet() {
        synchronized (gtidSetAccessLock) {
            return gtidSet != null ? gtidSet.toString() : null;
        }
    }

    /**
     * @param gtidStr GTID set string (can be an empty string).
     * <p>NOTE #1: Any value but null will switch BinaryLogClientExt into a GTID mode (this will also set binlogFilename
     * to "" (provided it's null) forcing MySQL to send events starting from the oldest known binlog (keep in mind
     * that connection will fail if gtid_purged is anything but empty (unless
     * {@link #setGtidSetFallbackToPurged(boolean)} is set to true))).
     * <p>NOTE #2: GTID set is automatically updated with each incoming GTID event (provided GTID mode is on).
     * @see #getGtidSet()
     * @see #setGtidSetFallbackToPurged(boolean)
     */
    public void setGtidSet(String gtidStr) {
        if ( gtidStr == null )
            return;

        this.gtidEnabled = true;

        if (this.binlogFilename == null) {
            this.binlogFilename = "";
        }

        synchronized (gtidSetAccessLock) {
            if ( !gtidStr.equals("") ) {
                if ( MariadbGtidSet.isMariaGtidSet(gtidStr) ) {
                    this.gtidSet = new MariadbGtidSet(gtidStr);
                } else {
                    this.gtidSet = new GtidSet(gtidStr);
                }
            }
        }
    }

    /**
     * @see #setGtidSetFallbackToPurged(boolean)
     * @return whether gtid_purged is used as a fallback
     */
    public boolean isGtidSetFallbackToPurged() {
        return gtidSetFallbackToPurged;
    }

    /**
     * @param gtidSetFallbackToPurged true if gtid_purged should be used as a fallback when gtidSet is set to "" and
     * MySQL server has purged some of the binary logs, false otherwise (default).
     */
    public void setGtidSetFallbackToPurged(boolean gtidSetFallbackToPurged) {
        this.gtidSetFallbackToPurged = gtidSetFallbackToPurged;
    }

    /**
     * @see #setUseBinlogFilenamePositionInGtidMode(boolean)
     * @return value of useBinlogFilenamePostionInGtidMode
     */
    public boolean isUseBinlogFilenamePositionInGtidMode() {
        return useBinlogFilenamePositionInGtidMode;
    }

    /**
     * @param useBinlogFilenamePositionInGtidMode true if MySQL server should start streaming events from a given
     * {@link #getBinlogFilename()} and {@link #getBinlogPosition()} instead of "the oldest known binlog" when
     * {@link #getGtidSet()} is set, false otherwise (default).
     */
    public void setUseBinlogFilenamePositionInGtidMode(boolean useBinlogFilenamePositionInGtidMode) {
        this.useBinlogFilenamePositionInGtidMode = useBinlogFilenamePositionInGtidMode;
    }

    /**
     * @return true if "keep alive" thread should be automatically started (default), false otherwise.
     * @see #setKeepAlive(boolean)
     */
    public boolean isKeepAlive() {
        return keepAlive;
    }

    /**
     * @param keepAlive true if "keep alive" thread should be automatically started (recommended and true by default),
     * false otherwise.
     * @see #isKeepAlive()
     * @see #setKeepAliveInterval(long)
     */
    public void setKeepAlive(boolean keepAlive) {
        this.keepAlive = keepAlive;
    }

    /**
     * @return "keep alive" interval in milliseconds, 1 minute by default.
     * @see #setKeepAliveInterval(long)
     */
    public long getKeepAliveInterval() {
        return keepAliveInterval;
    }

    /**
     * @param keepAliveInterval "keep alive" interval in milliseconds.
     * @see #getKeepAliveInterval()
     * @see #setHeartbeatInterval(long)
     */
    public void setKeepAliveInterval(long keepAliveInterval) {
        this.keepAliveInterval = keepAliveInterval;
    }

    /**
     * @return "keep alive" connect timeout in milliseconds.
     * @see #setKeepAliveConnectTimeout(long)
     *
     * @deprecated in favour of {@link #getConnectTimeout()}
     */
    public long getKeepAliveConnectTimeout() {
        return connectTimeout;
    }

    /**
     * @param connectTimeout "keep alive" connect timeout in milliseconds.
     * @see #getKeepAliveConnectTimeout()
     *
     * @deprecated in favour of {@link #setConnectTimeout(long)}
     */
    public void setKeepAliveConnectTimeout(long connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    /**
     * @return heartbeat period in milliseconds (0 if not set (default)).
     * @see #setHeartbeatInterval(long)
     */
    public long getHeartbeatInterval() {
        return heartbeatInterval;
    }

    /**
     * @param heartbeatInterval heartbeat period in milliseconds.
     * <p>
     * If set (recommended)
     * <ul>
     * <li> HEARTBEAT event will be emitted every "heartbeatInterval".
     * <li> if {@link #setKeepAlive(boolean)} is on then keepAlive thread will attempt to reconnect if no
     *   HEARTBEAT events were received within {@link #setKeepAliveInterval(long)} (instead of trying to send
     *   PING every {@link #setKeepAliveInterval(long)}, which is fundamentally flawed -
     *   https://github.com/shyiko/mysql-binlog-connector-java/issues/118).
     * </ul>
     * Note that when used together with keepAlive heartbeatInterval MUST be set less than keepAliveInterval.
     *
     * @see #getHeartbeatInterval()
     */
    public void setHeartbeatInterval(long heartbeatInterval) {
        this.heartbeatInterval = heartbeatInterval;
    }

    /**
     * @return connect timeout in milliseconds, 3 seconds by default.
     * @see #setConnectTimeout(long)
     */
    public long getConnectTimeout() {
        return connectTimeout;
    }

    /**
     * @param connectTimeout connect timeout in milliseconds.
     * @see #getConnectTimeout()
     */
    public void setConnectTimeout(long connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    /**
     * @param eventDeserializer custom event deserializer
     */
    public void setEventDeserializer(EventDeserializer eventDeserializer) {
        if (eventDeserializer == null) {
            throw new IllegalArgumentException("Event deserializer cannot be NULL");
        }
        this.eventDeserializer = eventDeserializer;
    }

    /**
     * @param socketFactory custom socket factory. If not provided, socket will be created with "new Socket()".
     */
    public void setSocketFactory(SocketFactory socketFactory) {
        this.socketFactory = socketFactory;
    }

    /**
     * @param sslSocketFactory custom ssl socket factory
     */
    public void setSslSocketFactory(SSLSocketFactory sslSocketFactory) {
        this.sslSocketFactory = sslSocketFactory;
    }

    /**
     * @param threadFactory custom thread factory. If not provided, threads will be created using simple "new Thread()".
     */
    public void setThreadFactory(ThreadFactory threadFactory) {
        this.threadFactory = threadFactory;
    }


    /**
     * @return true/false depending on whether we've connected to MariaDB.  NULL if not connected.
     */
    public Boolean getMariaDB() {
        if (databaseVersion != null) {
            return databaseVersion.isMariaDb();
        }
        return null;
    }

    public boolean isUseSendAnnotateRowsEvent() {
        return useSendAnnotateRowsEvent;
    }

    public void setUseSendAnnotateRowsEvent(boolean useSendAnnotateRowsEvent) {
        this.useSendAnnotateRowsEvent = useSendAnnotateRowsEvent;
    }

    /**
     * @return the configured MariaDB slave compatibility level, defaults to 4.
     */
    public int getMariaDbSlaveCapability() {
        return mariaDbSlaveCapability;
    }

    /**
     * Set the client's MariaDB slave compatibility level. This only applies when connecting to MariaDB.
     *
     * @param mariaDbSlaveCapability the expected compatibility level
     */
    public void setMariaDbSlaveCapability(int mariaDbSlaveCapability) {
        this.mariaDbSlaveCapability = mariaDbSlaveCapability;
    }

    /**
     * Connect to the replication stream. Note that this method blocks until disconnected.
     * @throws AuthenticationException if authentication fails
     * @throws ServerException if MySQL server responds with an error
     * @throws IOException if anything goes wrong while trying to connect
     * @throws IllegalStateException if binary log client is already connected
     */
    public void connect() throws IOException, IllegalStateException {
        if (!connectLock.tryLock()) {
            throw new IllegalStateException("BinaryLogClientExt is already connected");
        }
        boolean notifyWhenDisconnected = false;
        try {
            Callable cancelDisconnect = null;
            try {
                try {
                    long start = System.currentTimeMillis();
                    channel = openChannel();
                    if (connectTimeout > 0 && !isKeepAliveThreadRunning()) {
                        cancelDisconnect = scheduleDisconnectIn(connectTimeout -
                                (System.currentTimeMillis() - start));
                    }
                    if (channel.getInputStream().peek() == -1) {
                        throw new EOFException();
                    }
                } catch (IOException e) {
                    throw new IOException("Failed to connect to MySQL on " + hostname + ":" + port +
                            ". Please make sure it's running.", e);
                }
                GreetingPacket greetingPacket = receiveGreeting();

                resolveDatabaseVersion(greetingPacket);
                tryUpgradeToSSL(greetingPacket);

                new Authenticator(greetingPacket, channel, schema, username, password).authenticate();
                channel.authenticationComplete();

                connectionId = greetingPacket.getThreadId();
                if ("".equals(binlogFilename)) {
                    setupGtidSet();
                }
                if (binlogFilename == null) {
                    fetchBinlogFilenameAndPosition();
                }
                if (binlogPosition < 4) {
                    if (logger.isWarnEnabled()) {
                        logger.warn("Binary log position adjusted from " + binlogPosition + " to " + 4);
                    }
                    binlogPosition = 4;
                }
                setupConnection();
                gtid = null;
                tx = false;
                requestBinaryLogStream();
            } catch (IOException e) {
                disconnectChannel();
                throw e;
            } finally {
                if (cancelDisconnect != null) {
                    try {
                        cancelDisconnect.call();
                    } catch (Exception e) {
                        if (logger.isWarnEnabled()) {
                            logger.warn("\"" + e.getMessage() +
                                    "\" was thrown while canceling scheduled disconnect call");
                        }
                    }
                }
            }
            connected = true;
            notifyWhenDisconnected = true;
            if (logger.isInfoEnabled()) {
                String position;
                synchronized (gtidSetAccessLock) {
                    position = gtidSet != null ? gtidSet.toString() : binlogFilename + "/" + binlogPosition;
                }
                logger.info("Connected to " + hostname + ":" + port + " at " + position +
                        " (" + (blocking ? "sid:" + serverId + ", " : "") + "cid:" + connectionId + ")");
            }
            for (BinaryLogClientExt.LifecycleListener lifecycleListener : lifecycleListeners) {
                lifecycleListener.onConnect(this);
            }
            if (keepAlive && !isKeepAliveThreadRunning()) {
                spawnKeepAliveThread();
            }
            ensureEventDataDeserializer(EventType.ROTATE, RotateEventDataDeserializer.class);
            synchronized (gtidSetAccessLock) {
                if (this.gtidEnabled) {
                    ensureGtidEventDataDeserializer();
                }
            }
            listenForEventPackets();
        } finally {
            connectLock.unlock();
            if (notifyWhenDisconnected) {
                for (BinaryLogClientExt.LifecycleListener lifecycleListener : lifecycleListeners) {
                    lifecycleListener.onDisconnect(this);
                }
            }
        }
    }

    private void resolveDatabaseVersion(GreetingPacket packet) {
        this.databaseVersion = BinaryLogDatabaseVersion.parse(packet.getServerVersion());
        logger.info("Database version: " + this.databaseVersion);
    }

    /**
     * Apply additional options for connection before requesting binlog stream.
     */
    protected void setupConnection() throws IOException {
        ChecksumType checksumType = fetchBinlogChecksum();
        if (checksumType != ChecksumType.NONE) {
            confirmSupportOfChecksum(checksumType);
        }
        setMasterServerId();
        if (heartbeatInterval > 0) {
            enableHeartbeat();
        }
    }

    private PacketChannel openChannel() throws IOException {
        Socket socket = socketFactory != null ? socketFactory.createSocket() : new Socket();
        socket.connect(new InetSocketAddress(hostname, port), (int) connectTimeout);
        return new PacketChannel(socket);
    }

    private Callable scheduleDisconnectIn(final long timeout) {
        final BinaryLogClientExt self = this;
        final CountDownLatch connectLatch = new CountDownLatch(1);
        final Thread thread = newNamedThread(new Runnable() {
            @Override
            public void run() {
                try {
                    connectLatch.await(timeout, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    if (logger.isWarnEnabled()) {
                        logger.warn( e.getMessage(),e);
                    }
                }
                if (connectLatch.getCount() != 0) {
                    if (logger.isWarnEnabled()) {
                        logger.warn("Failed to establish connection in " + timeout + "ms. " +
                                "Forcing disconnect.");
                    }
                    try {
                        self.disconnectChannel();
                    } catch (IOException e) {
                        if (logger.isWarnEnabled()) {
                            logger.warn( e.getMessage(),e);
                        }
                    }
                }
            }
        }, "blc-disconnect-" + hostname + ":" + port);
        thread.start();
        return new Callable() {

            public Object call() throws Exception {
                connectLatch.countDown();
                thread.join();
                return null;
            }
        };
    }

    protected void checkError(byte[] packet) throws IOException {
        if (packet[0] == (byte) 0xFF /* error */) {
            byte[] bytes = Arrays.copyOfRange(packet, 1, packet.length);
            ErrorPacket errorPacket = new ErrorPacket(bytes);
            throw new ServerException(errorPacket.getErrorMessage(), errorPacket.getErrorCode(),
                    errorPacket.getSqlState());
        }
    }

    private GreetingPacket receiveGreeting() throws IOException {
        byte[] initialHandshakePacket = channel.read();
        checkError(initialHandshakePacket);

        return new GreetingPacket(initialHandshakePacket);
    }

    private boolean tryUpgradeToSSL(GreetingPacket greetingPacket) throws IOException {
        int collation = greetingPacket.getServerCollation();

        if (sslMode != SSLMode.DISABLED) {
            boolean serverSupportsSSL = (greetingPacket.getServerCapabilities() & ClientCapabilities.SSL) != 0;
            if (!serverSupportsSSL && (sslMode == SSLMode.REQUIRED || sslMode == SSLMode.VERIFY_CA ||
                    sslMode == SSLMode.VERIFY_IDENTITY)) {
                throw new IOException("MySQL server does not support SSL");
            }
            if (serverSupportsSSL) {
                SSLRequestCommand sslRequestCommand = new SSLRequestCommand();
                sslRequestCommand.setCollation(collation);
                channel.write(sslRequestCommand);
                SSLSocketFactory sslSocketFactory =
                        this.sslSocketFactory != null ?
                                this.sslSocketFactory :
                                sslMode == SSLMode.REQUIRED || sslMode == SSLMode.PREFERRED ?
                                        DEFAULT_REQUIRED_SSL_MODE_SOCKET_FACTORY :
                                        DEFAULT_VERIFY_CA_SSL_MODE_SOCKET_FACTORY;
                channel.upgradeToSSL(sslSocketFactory,
                        sslMode == SSLMode.VERIFY_IDENTITY ? new TLSHostnameVerifier() : null);
                logger.info("SSL enabled");
                return true;
            }
        }
        return false;
    }

    private void enableHeartbeat() throws IOException {
        channel.write(new QueryCommand("set @master_heartbeat_period=" + heartbeatInterval * 1000000));
        byte[] statementResult = channel.read();
        checkError(statementResult);
    }

    private void setMasterServerId() throws IOException {
        channel.write(new QueryCommand("select @@server_id"));
        ResultSetRowPacket[] resultSet = readResultSet();
        if (resultSet.length >= 0) {
            this.masterServerId = Long.parseLong(resultSet[0].getValue(0));
        }
    }

    protected void requestBinaryLogStream() throws IOException {
        long serverId = blocking ? this.serverId : 0; // http://bugs.mysql.com/bug.php?id=71178
        if ( this.databaseVersion.isMariaDb() )
            requestBinaryLogStreamMaria(serverId);
        else
            requestBinaryLogStreamMysql(serverId);
    }

    private void requestBinaryLogStreamMysql(long serverId) throws IOException {
        Command dumpBinaryLogCommand;
        synchronized (gtidSetAccessLock) {
            if (this.gtidEnabled) {
                dumpBinaryLogCommand = new DumpBinaryLogGtidCommand(serverId,
                        useBinlogFilenamePositionInGtidMode ? binlogFilename : "",
                        useBinlogFilenamePositionInGtidMode ? binlogPosition : 4,
                        gtidSet);
            } else {
                dumpBinaryLogCommand = new DumpBinaryLogCommand(serverId, binlogFilename, binlogPosition);
            }
        }
        channel.write(dumpBinaryLogCommand);
    }

    protected void requestBinaryLogStreamMaria(long serverId) throws IOException {
        Command dumpBinaryLogCommand;

        /*
            https://jira.mariadb.org/browse/MDEV-225
         */
        channel.write(new QueryCommand("SET @mariadb_slave_capability=" + mariaDbSlaveCapability));
        checkError(channel.read());

        synchronized (gtidSetAccessLock) {
            if (this.gtidEnabled) {
                logger.info(gtidSet.toString());
                channel.write(new QueryCommand("SET @slave_connect_state = '" + gtidSet.toString() + "'"));
                checkError(channel.read());
                channel.write(new QueryCommand("SET @slave_gtid_strict_mode = 0"));
                checkError(channel.read());
                channel.write(new QueryCommand("SET @slave_gtid_ignore_duplicates = 0"));
                checkError(channel.read());
                dumpBinaryLogCommand = new DumpBinaryLogCommand(serverId, "", 0L, isUseSendAnnotateRowsEvent());
            } else {
                dumpBinaryLogCommand = new DumpBinaryLogCommand(serverId, binlogFilename, binlogPosition, isUseSendAnnotateRowsEvent());
            }
        }
        channel.write(dumpBinaryLogCommand);
    }

    protected void ensureEventDataDeserializer(EventType eventType,
                                               Class<? extends EventDataDeserializer> eventDataDeserializerClass) {
        EventDataDeserializer eventDataDeserializer = eventDeserializer.getEventDataDeserializer(eventType);
        if (eventDataDeserializer.getClass() != eventDataDeserializerClass &&
                eventDataDeserializer.getClass() != EventDeserializer.EventDataWrapper.Deserializer.class) {
            EventDataDeserializer internalEventDataDeserializer;
            try {
                internalEventDataDeserializer = eventDataDeserializerClass.newInstance();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            eventDeserializer.setEventDataDeserializer(eventType,
                    new EventDeserializer.EventDataWrapper.Deserializer(internalEventDataDeserializer,
                            eventDataDeserializer));
        }
    }

    protected void ensureGtidEventDataDeserializer() {
        ensureEventDataDeserializer(EventType.GTID, GtidEventDataDeserializer.class);
        ensureEventDataDeserializer(EventType.QUERY, QueryEventDataDeserializer.class);
        ensureEventDataDeserializer(EventType.ANNOTATE_ROWS, AnnotateRowsEventDataDeserializer.class);
        ensureEventDataDeserializer(EventType.MARIADB_GTID, MariadbGtidEventDataDeserializer.class);
        ensureEventDataDeserializer(EventType.MARIADB_GTID_LIST, MariadbGtidListEventDataDeserializer.class);
    }

    private void spawnKeepAliveThread() {
        final ExecutorService threadExecutor =
                Executors.newSingleThreadExecutor(new ThreadFactory() {

                    @Override
                    public Thread newThread(Runnable runnable) {
                        return newNamedThread(runnable, "blc-keepalive-" + hostname + ":" + port);
                    }
                });
        try {
            keepAliveThreadExecutorLock.lock();
            threadExecutor.submit(new Runnable() {
                @Override
                public void run() {
                    while (!threadExecutor.isShutdown()) {
                        try {
                            Thread.sleep(keepAliveInterval);
                        } catch (InterruptedException e) {
                            // expected in case of disconnect
                        }
                        if (threadExecutor.isShutdown()) {
                            logger.info("threadExecutor is shut down, terminating keepalive thread");
                            return;
                        }
                        boolean connectionLost = false;
                        if (heartbeatInterval > 0) {
                            connectionLost = System.currentTimeMillis() - eventLastSeen > keepAliveInterval;
                        } else {
                            try {
                                channel.write(new PingCommand());
                            } catch (IOException e) {
                                logger.warn("Keepalive: Failed to ping " + hostname + ":" + port ,e);
                                connectionLost = true;
                            }
                        }
                        if (connectionLost) {
                            logger.info("Keepalive: Trying to restore lost connection to " + hostname + ":" + port);
                            try {
                                terminateConnect(useNonGracefulDisconnect);
                                connect(connectTimeout);
                            } catch (Exception ce) {
                                logger.warn("keepalive: Failed to restore connection to " + hostname + ":" + port +
                                        ". Next attempt in " + keepAliveInterval + "ms",ce);
                            }
                        }
                    }
                }
            });
            keepAliveThreadExecutor = threadExecutor;
        } finally {
            keepAliveThreadExecutorLock.unlock();
        }
    }

    private Thread newNamedThread(Runnable runnable, String threadName) {
        Thread thread = threadFactory == null ? new Thread(runnable) : threadFactory.newThread(runnable);
        thread.setName(threadName);
        return thread;
    }

    boolean isKeepAliveThreadRunning() {
        try {
            keepAliveThreadExecutorLock.lock();
            return keepAliveThreadExecutor != null && !keepAliveThreadExecutor.isShutdown();
        } finally {
            keepAliveThreadExecutorLock.unlock();
        }
    }

    /**
     * Connect to the replication stream in a separate thread.
     * @param timeout timeout in milliseconds
     * @throws AuthenticationException if authentication fails
     * @throws ServerException if MySQL server responds with an error
     * @throws IOException if anything goes wrong while trying to connect
     * @throws TimeoutException if client was unable to connect within given time limit
     */
    public void connect(final long timeout) throws IOException, TimeoutException {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        BinaryLogClientExt.AbstractLifecycleListener connectListener = new BinaryLogClientExt.AbstractLifecycleListener() {
            @Override
            public void onConnect(BinaryLogClientExt client) {
                countDownLatch.countDown();
            }
        };
        registerLifecycleListener(connectListener);
        final AtomicReference<IOException> exceptionReference = new AtomicReference<IOException>();
        Runnable runnable = new Runnable() {

            @Override
            public void run() {
                try {
                    setConnectTimeout(timeout);
                    connect();
                } catch (IOException e) {
                    exceptionReference.set(e);
                    countDownLatch.countDown(); // making sure we don't end up waiting whole "timeout"
                } catch (Exception e) {
                    exceptionReference.set(new IOException(e)); // method is asynchronous, catch all exceptions so that they are not lost
                    countDownLatch.countDown(); // making sure we don't end up waiting whole "timeout"
                }
            }
        };
        newNamedThread(runnable, "blc-" + hostname + ":" + port).start();
        boolean started = false;
        try {
            started = countDownLatch.await(timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            if (logger.isWarnEnabled()) {
                logger.warn( e.getMessage(),e);
            }
        }
        unregisterLifecycleListener(connectListener);
        if (exceptionReference.get() != null) {
            throw exceptionReference.get();
        }
        if (!started) {
            try {
                terminateConnect();
            } finally {
                throw new TimeoutException("BinaryLogClientExt was unable to connect in " + timeout + "ms");
            }
        }
    }

    /**
     * @return true if client is connected, false otherwise
     */
    public boolean isConnected() {
        return connected;
    }

    private String fetchGtidPurged() throws IOException {
        channel.write(new QueryCommand("show global variables like 'gtid_purged'"));
        ResultSetRowPacket[] resultSet = readResultSet();
        if (resultSet.length != 0) {
            return resultSet[0].getValue(1).toUpperCase();
        }
        return "";
    }

    protected void setupGtidSet() throws IOException{
        if (!this.gtidEnabled)
            return;

        synchronized (gtidSetAccessLock) {
            if ( this.databaseVersion.isMariaDb() ) {
                if ( gtidSet == null ) {
                    gtidSet = new MariadbGtidSet("");
                } else if ( !(gtidSet instanceof MariadbGtidSet) ) {
                    throw new RuntimeException("Connected to MariaDB but given a mysql GTID set!");
                }
            } else {
                if ( gtidSet == null && gtidSetFallbackToPurged ) {
                    gtidSet = new GtidSet(fetchGtidPurged());
                } else if ( gtidSet == null ){
                    gtidSet = new GtidSet("");
                } else if ( gtidSet instanceof MariadbGtidSet ) {
                    throw new RuntimeException("Connected to Mysql but given a MariaDB GTID set!");
                }
            }
        }

    }

    private void fetchBinlogFilenameAndPosition() throws IOException {
        ResultSetRowPacket[] resultSet;
        if (!databaseVersion.isMariaDb() && databaseVersion.isGreaterThanOrEqualTo(8, 4)) {
            channel.write(new QueryCommand("show binary log status"));
        } else {
            channel.write(new QueryCommand("show master status"));
        }
        resultSet = readResultSet();
        if (resultSet.length == 0) {
            throw new IOException("Failed to determine binlog filename/position");
        }
        ResultSetRowPacket resultSetRow = resultSet[0];
        binlogFilename = resultSetRow.getValue(0);
        binlogPosition = Long.parseLong(resultSetRow.getValue(1));
    }

    private ChecksumType fetchBinlogChecksum() throws IOException {
        channel.write(new QueryCommand("show global variables like 'binlog_checksum'"));
        ResultSetRowPacket[] resultSet = readResultSet();
        if (resultSet.length == 0) {
            return ChecksumType.NONE;
        }
        return ChecksumType.valueOf(resultSet[0].getValue(1).toUpperCase());
    }

    private void confirmSupportOfChecksum(ChecksumType checksumType) throws IOException {
        channel.write(new QueryCommand("set @master_binlog_checksum= @@global.binlog_checksum"));
        byte[] statementResult = channel.read();
        checkError(statementResult);
        eventDeserializer.setChecksumType(checksumType);
    }

    private void listenForEventPackets() throws IOException {
        ByteArrayInputStream inputStream = channel.getInputStream();
        boolean completeShutdown = false;
        try {
            while (inputStream.peek() != -1) {
                int packetLength = inputStream.readInteger(3);
                inputStream.skip(1); // 1 byte for sequence
                int marker = inputStream.read();
                if (marker == 0xFF) {
                    ErrorPacket errorPacket = new ErrorPacket(inputStream.read(packetLength - 1));
                    throw new ServerException(errorPacket.getErrorMessage(), errorPacket.getErrorCode(),
                            errorPacket.getSqlState());
                }
                if (marker == 0xFE && !blocking) {
                    completeShutdown = true;
                    break;
                }
                Event event;
                try {
                    event = eventDeserializer.nextEvent(packetLength == MAX_PACKET_LENGTH ?
                            new ByteArrayInputStream(readPacketSplitInChunks(inputStream, packetLength - 1)) :
                            inputStream);
                    if (event == null) {
                        throw new EOFException();
                    }
                } catch (Exception e) {
                    Throwable cause = e instanceof EventDataDeserializationException ? e.getCause() : e;
                    if (cause instanceof EOFException || cause instanceof SocketException) {
                        throw e;
                    }
                    if (isConnected()) {
                        for (BinaryLogClientExt.LifecycleListener lifecycleListener : lifecycleListeners) {
                            lifecycleListener.onEventDeserializationFailure(this, e);
                        }
                    }
                    continue;
                }
                if (isConnected()) {
                    eventLastSeen = System.currentTimeMillis();
                    updateGtidSet(event);
                    notifyEventListeners(event);
                    updateClientBinlogFilenameAndPosition(event);
                }
            }
        } catch (Exception e) {
            if (isConnected()) {
                for (BinaryLogClientExt.LifecycleListener lifecycleListener : lifecycleListeners) {
                    lifecycleListener.onCommunicationFailure(this, e);
                }
            }
        } finally {
            if (isConnected()) {
                if (completeShutdown) {
                    disconnect(); // initiate complete shutdown sequence (which includes keep alive thread)
                } else {
                    disconnectChannel();
                }
            }
        }
    }

    private byte[] readPacketSplitInChunks(ByteArrayInputStream inputStream, int packetLength) throws IOException {
        byte[] result = inputStream.read(packetLength);
        int chunkLength;
        do {
            chunkLength = inputStream.readInteger(3);
            inputStream.skip(1); // 1 byte for sequence
            result = Arrays.copyOf(result, result.length + chunkLength);
            inputStream.fill(result, result.length - chunkLength, chunkLength);
        } while (chunkLength == Packet.MAX_LENGTH);
        return result;
    }

    private void updateClientBinlogFilenameAndPosition(Event event) {
        EventHeader eventHeader = event.getHeader();
        EventType eventType = eventHeader.getEventType();
        if (eventType == EventType.ROTATE) {
            RotateEventData rotateEventData = (RotateEventData) EventDeserializer.EventDataWrapper.internal(event.getData());
            binlogFilename = rotateEventData.getBinlogFilename();
            binlogPosition = rotateEventData.getBinlogPosition();
        } else
            // do not update binlogPosition on TABLE_MAP so that in case of reconnect (using a different instance of
            // client) table mapping cache could be reconstructed before hitting row mutation event
            if (eventType != EventType.TABLE_MAP && eventHeader instanceof EventHeaderV4) {
                EventHeaderV4 trackableEventHeader = (EventHeaderV4) eventHeader;
                long nextBinlogPosition = trackableEventHeader.getNextPosition();
                if (nextBinlogPosition > 0) {
                    binlogPosition = nextBinlogPosition;
                }
            }
    }

    protected void updateGtidSet(Event event) {
        synchronized (gtidSetAccessLock) {
            if (gtidSet == null) {
                return;
            }
        }
        EventHeader eventHeader = event.getHeader();
        switch(eventHeader.getEventType()) {
            case GTID:
                GtidEventData gtidEventData = (GtidEventData) EventDeserializer.EventDataWrapper.internal(event.getData());
                gtid = gtidEventData.getMySqlGtid();
                break;
            case XID:
                commitGtid();
                tx = false;
                break;
            case QUERY:
                QueryEventData queryEventData = (QueryEventData) EventDeserializer.EventDataWrapper.internal(event.getData());
                String sql = queryEventData.getSql();
                if (sql == null) {
                    break;
                }
                commitGtid(sql);
                break;
            case ANNOTATE_ROWS:
                AnnotateRowsEventData annotateRowsEventData = (AnnotateRowsEventData) EventDeserializer.EventDataWrapper.internal(event.getData());
                sql = annotateRowsEventData.getRowsQuery();
                if (sql == null) {
                    break;
                }
                commitGtid(sql);
                break;
            case MARIADB_GTID:
                MariadbGtidEventData mariadbGtidEventData = (MariadbGtidEventData) EventDeserializer.EventDataWrapper.internal(event.getData());
                mariadbGtidEventData.setServerId(eventHeader.getServerId());
                gtid = mariadbGtidEventData.toString();
                break;
            case MARIADB_GTID_LIST:
                MariadbGtidListEventData mariadbGtidListEventData = (MariadbGtidListEventData) EventDeserializer.EventDataWrapper.internal(event.getData());
                gtid = mariadbGtidListEventData.getMariaGTIDSet().toString();
                break;
            default:
        }
    }

    protected void commitGtid(String sql) {
        if ("BEGIN".equals(sql)) {
            tx = true;
        } else
        if ("COMMIT".equals(sql) || "ROLLBACK".equals(sql)) {
            commitGtid();
            tx = false;
        } else
        if (!tx) {
            // auto-commit query, likely DDL
            commitGtid();
        }
    }

    private void commitGtid() {
        if (gtid != null) {
            synchronized (gtidSetAccessLock) {
                gtidSet.addGtid(gtid);
            }
        }
    }

    private ResultSetRowPacket[] readResultSet() throws IOException {
        List<ResultSetRowPacket> resultSet = new LinkedList<>();
        byte[] statementResult = channel.read();
        checkError(statementResult);

        while ((channel.read())[0] != (byte) 0xFE /* eof */) { /* skip */ }
        for (byte[] bytes; (bytes = channel.read())[0] != (byte) 0xFE /* eof */; ) {
            checkError(bytes);
            resultSet.add(new ResultSetRowPacket(bytes));
        }
        return resultSet.toArray(new ResultSetRowPacket[resultSet.size()]);
    }

    /**
     * @return registered event listeners
     */
    public List<BinaryLogClientExt.EventListener> getEventListeners() {
        return Collections.unmodifiableList(eventListeners);
    }

    /**
     * Register event listener. Note that multiple event listeners will be called in order they
     * where registered.
     * @param eventListener event listener
     */
    public void registerEventListener(BinaryLogClientExt.EventListener eventListener) {
        eventListeners.add(eventListener);
    }

    /**
     * Unregister all event listener of specific type.
     * @param listenerClass event listener class to unregister
     */
    public void unregisterEventListener(Class<? extends BinaryLogClientExt.EventListener> listenerClass) {
        for (BinaryLogClientExt.EventListener eventListener: eventListeners) {
            if (listenerClass.isInstance(eventListener)) {
                eventListeners.remove(eventListener);
            }
        }
    }

    /**
     * Unregister single event listener.
     * @param eventListener event listener to unregister
     */
    public void unregisterEventListener(BinaryLogClientExt.EventListener eventListener) {
        eventListeners.remove(eventListener);
    }

    private void notifyEventListeners(Event event) {
        if (event.getData() instanceof EventDeserializer.EventDataWrapper) {
            event = new Event(event.getHeader(), ((EventDeserializer.EventDataWrapper) event.getData()).getExternal());
        }
        for (BinaryLogClientExt.EventListener eventListener : eventListeners) {
            try {
                eventListener.onEvent(event);
            } catch (Exception e) {
                if (logger.isWarnEnabled()) {
                    logger.warn( eventListener + " choked on " + event, e);
                }
            }
        }
    }

    /**
     * @return registered lifecycle listeners
     */
    public List<BinaryLogClientExt.LifecycleListener> getLifecycleListeners() {
        return Collections.unmodifiableList(lifecycleListeners);
    }

    /**
     * Register lifecycle listener. Note that multiple lifecycle listeners will be called in order they
     * where registered.
     * @param lifecycleListener lifecycle listener to register
     */
    public void registerLifecycleListener(BinaryLogClientExt.LifecycleListener lifecycleListener) {
        lifecycleListeners.add(lifecycleListener);
    }

    /**
     * Unregister all lifecycle listener of specific type.
     * @param listenerClass lifecycle listener class to unregister
     */
    public void unregisterLifecycleListener(Class<? extends BinaryLogClientExt.LifecycleListener> listenerClass) {
        for (BinaryLogClientExt.LifecycleListener lifecycleListener : lifecycleListeners) {
            if (listenerClass.isInstance(lifecycleListener)) {
                lifecycleListeners.remove(lifecycleListener);
            }
        }
    }

    /**
     * Unregister single lifecycle listener.
     * @param eventListener lifecycle listener to unregister
     */
    public void unregisterLifecycleListener(BinaryLogClientExt.LifecycleListener eventListener) {
        lifecycleListeners.remove(eventListener);
    }

    /**
     * Disconnect from the replication stream.
     * Note that this does not cause binlogFilename/binlogPosition to be cleared out.
     * As the result following {@link #connect()} resumes client from where it left off.
     */
    public void disconnect() throws IOException {
        terminateKeepAliveThread();
        terminateConnect();
    }

    private void terminateKeepAliveThread() {
        try {
            keepAliveThreadExecutorLock.lock();
            ExecutorService keepAliveThreadExecutor = this.keepAliveThreadExecutor;
            if ( keepAliveThreadExecutor == null ) {
                return;
            }
            keepAliveThreadExecutor.shutdownNow();
        } finally {
            keepAliveThreadExecutorLock.unlock();
        }
        while (!awaitTerminationInterruptibly(keepAliveThreadExecutor,
                Long.MAX_VALUE, TimeUnit.NANOSECONDS)) {
            // ignore
        }
    }

    private static boolean awaitTerminationInterruptibly(ExecutorService executorService, long timeout, TimeUnit unit) {
        try {
            return executorService.awaitTermination(timeout, unit);
        } catch (InterruptedException e) {
            return false;
        }
    }

    private void terminateConnect() throws IOException {
        terminateConnect(false);
    }
    private void terminateConnect(boolean force) throws IOException {
        do {
            disconnectChannel(force);
        } while (!tryLockInterruptibly(connectLock, 1000, TimeUnit.MILLISECONDS));
        connectLock.unlock();
    }

    private static boolean tryLockInterruptibly(Lock lock, long time, TimeUnit unit) {
        try {
            return lock.tryLock(time, unit);
        } catch (InterruptedException e) {
            return false;
        }
    }

    private void disconnectChannel() throws IOException {
        disconnectChannel(false);
    }
    private void disconnectChannel(boolean force) throws IOException {
        connected = false;
        if (channel != null && channel.isOpen()) {
            if (force) {
                channel.setShouldUseSoLinger0();
            }
            channel.close();
        }
    }

    /**
     * {@link BinaryLogClientExt}'s event listener.
     */
    public interface EventListener {

        void onEvent(Event event);
    }

    /**
     * {@link BinaryLogClientExt}'s lifecycle listener.
     */
    public interface LifecycleListener {

        /**
         * Called once client has successfully logged in but before started to receive binlog events.
         * @param client the client that logged in
         */
        void onConnect(BinaryLogClientExt client);

        /**
         * It's guarantied to be called before {@link #onDisconnect(BinaryLogClientExt)}) in case of
         * communication failure.
         * @param client the client that triggered the communication failure
         * @param ex The exception that triggered the communication failutre
         */
        void onCommunicationFailure(BinaryLogClientExt client, Exception ex);

        /**
         * Called in case of failed event deserialization. Note this type of error does NOT cause client to
         * disconnect. If you wish to stop receiving events you'll need to fire client.disconnect() manually.
         * @param client the client that failed event deserialization
         * @param ex The exception that triggered the failutre
         */
        void onEventDeserializationFailure(BinaryLogClientExt client, Exception ex);

        /**
         * Called upon disconnect (regardless of the reason).
         * @param client the client that disconnected
         */
        void onDisconnect(BinaryLogClientExt client);
    }

    /**
     * Default (no-op) implementation of {@link BinaryLogClientExt.LifecycleListener}.
     */
    public static abstract class AbstractLifecycleListener implements BinaryLogClientExt.LifecycleListener {

        public void onConnect(BinaryLogClientExt client) { }

        public void onCommunicationFailure(BinaryLogClientExt client, Exception ex) { }

        public void onEventDeserializationFailure(BinaryLogClientExt client, Exception ex) { }

        public void onDisconnect(BinaryLogClientExt client) { }

    }

}
