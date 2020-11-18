package com.github.dongchan;

import com.github.dongchan.network.*;
import com.github.dongchan.network.protocol.GreetingPacket;
import com.github.dongchan.network.protocol.PacketChannel;
import com.github.dongchan.network.protocol.ResultSetRowPacket;
import com.github.dongchan.network.protocol.command.QueryCommand;
import com.github.dongchan.network.protocol.command.SSLRequestCommand;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.security.GeneralSecurityException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author DongChan
 * @date 2020/11/16
 * @time 5:28 PM
 */
public class MySQLClient {

    private static final SSLSocketFactory DEFAULT_REQUIRED_SSL_MODE_SOCKET_FACTORY = new DefaultSSLSocketFactory() {

        @Override
        protected void initSSLContext(SSLContext sc) throws GeneralSecurityException {
            sc.init(null, new TrustManager[]{
                    new X509TrustManager() {

                        @Override
                        public void checkClientTrusted(X509Certificate[] x509Certificates, String s)
                                throws CertificateException {
                        }

                        @Override
                        public void checkServerTrusted(X509Certificate[] x509Certificates, String s)
                                throws CertificateException {
                        }

                        @Override
                        public X509Certificate[] getAcceptedIssuers() {
                            return new X509Certificate[0];
                        }
                    }
            }, null);
        }
    };
    private static final SSLSocketFactory DEFAULT_VERIFY_CA_SSL_MODE_SOCKET_FACTORY = new DefaultSSLSocketFactory();
    private final String hostname;
    private final int port;
    private final String schema;
    private final String username;
    private final String password;
    private final Lock connectLock = new ReentrantLock();
    private final Lock keepAliveThreadExecutorLock = new ReentrantLock();
    private final Logger logger = Logger.getLogger(getClass().getName());
    private PacketChannel channel;
    private long connectTimeout = TimeUnit.SECONDS.toMillis(3);
    private SSLMode sslMode = SSLMode.DISABLED;
    private SSLSocketFactory sslSocketFactory;
    private volatile long connectionId;
    private volatile String binlogFilename;
    private volatile long binlogPosition = 4;
    private volatile ExecutorService keepAliveThreadExecutor;
    private volatile boolean connected;
    private ThreadFactory threadFactory;


    public MySQLClient(String hostname, int port, String schema, String username, String password) throws IOException {
        this.hostname = hostname;
        this.port = port;
        this.schema = schema;
        this.username = username;
        this.password = password;
        this.channel = openChannel();
    }

    /**
     * Connect to the replication stream. Note that this method blocks until disconnected.
     *
     * @throws AuthenticationException if authentication fails
     * @throws ServerException         if MySQL server responds with an error
     * @throws IOException             if anything goes wrong while trying to connect
     * @throws IllegalStateException   if binary log client is already connected
     */
    public void connect() throws IOException, IllegalStateException {
        if (!connectLock.tryLock()) {
            throw new IllegalStateException("BinaryLogClient is already connected");
        }
        boolean notifyWhenDisconnected = false;
        try {
            Callable cancelDisconnect = null;
            try {
                try {
                    long start = System.currentTimeMillis();
                    channel = openChannel();
                    if (connectTimeout > 0 && !isKeepAliveThreadRunning()) {
//                        cancelDisconnect = scheduleDisconnectIn(connectTimeout -
//                                (System.currentTimeMillis() - start));
                        System.out.println("ConnectTimeout > 0");
                    }
                    if (channel.getInputStream().peek() == -1) {
                        throw new EOFException();
                    }
                } catch (IOException e) {
                    throw new IOException("Failed to connect to MySQL on " + hostname + ":" + port +
                            ". Please make sure it's running.", e);
                }
                GreetingPacket greetingPacket = receiveGreeting();

                tryUpgradeToSSL(greetingPacket);

                new Authenticator(greetingPacket, channel, schema, username, password).authenticate();
                channel.authenticationComplete();

                connectionId = greetingPacket.getThreadId();
                if ("".equals(binlogFilename)) {
//                    synchronized (gtidSetAccessLock) {
//                        if (gtidSet != null && "".equals(gtidSet.toString()) && gtidSetFallbackToPurged) {
//                            gtidSet = new GtidSet(fetchGtidPurged());
//                        }
//                    }
                }
                if (binlogFilename == null) {
                    fetchBinlogFilenameAndPosition();
                }
                if (binlogPosition < 4) {
                    if (logger.isLoggable(Level.WARNING)) {
                        logger.warning("Binary log position adjusted from " + binlogPosition + " to " + 4);
                    }
                    binlogPosition = 4;
                }
//                ChecksumType checksumType = fetchBinlogChecksum();
//                if (checksumType != ChecksumType.NONE) {
//                    confirmSupportOfChecksum(checksumType);
//                }
//                setMasterServerId();
//                if (heartbeatInterval > 0) {
//                    enableHeartbeat();
//                }
            } catch (IOException e) {
//                disconnectChannel();
                throw e;
            } finally {
                if (cancelDisconnect != null) {
                    try {
                        cancelDisconnect.call();
                    } catch (Exception e) {
                        if (logger.isLoggable(Level.WARNING)) {
                            logger.warning("\"" + e.getMessage() +
                                    "\" was thrown while canceling scheduled disconnect call");
                        }
                    }
                }
            }
        } finally {
            connectLock.unlock();
        }
    }


    /**
     * Connect to the replication stream. Note that this method blocks until disconnected.
     *
     * @throws AuthenticationException if authentication fails
     * @throws ServerException         if MySQL server responds with an error
     * @throws IOException             if anything goes wrong while trying to connect
     * @throws IllegalStateException   if binary log client is already connected
     */
    public void connectAndFetchStatus() throws IOException, IllegalStateException {
        if (!connectLock.tryLock()) {
            throw new IllegalStateException("BinaryLogClient is already connected");
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

                tryUpgradeToSSL(greetingPacket);

                new Authenticator(greetingPacket, channel, schema, username, password).authenticate();
                channel.authenticationComplete();

                connectionId = greetingPacket.getThreadId();
                if ("".equals(binlogFilename)) {
//                    synchronized (gtidSetAccessLock) {
//                        if (gtidSet != null && "".equals(gtidSet.toString()) && gtidSetFallbackToPurged) {
//                            gtidSet = new GtidSet(fetchGtidPurged());
//                        }
//                    }
                }
                if (binlogFilename == null) {
                    fetchBinlogFilenameAndPosition();
                }
                if (binlogPosition < 4) {
                    if (logger.isLoggable(Level.WARNING)) {
                        logger.warning("Binary log position adjusted from " + binlogPosition + " to " + 4);
                    }
                    binlogPosition = 4;
                }
//                ChecksumType checksumType = fetchBinlogChecksum();
//                if (checksumType != ChecksumType.NONE) {
//                    confirmSupportOfChecksum(checksumType);
//                }
//                setMasterServerId();
//                if (heartbeatInterval > 0) {
//                    enableHeartbeat();
//                }
            } catch (IOException e) {
                disconnectChannel();
                throw e;
            } finally {
                if (cancelDisconnect != null) {
                    try {
                        cancelDisconnect.call();
                    } catch (Exception e) {
                        if (logger.isLoggable(Level.WARNING)) {
                            logger.warning("\"" + e.getMessage() +
                                    "\" was thrown while canceling scheduled disconnect call");
                        }
                    }
                }
            }
        } finally {
            connectLock.unlock();
        }
    }

    private void disconnectChannel() throws IOException {
        connected = false;
        if (channel != null && channel.isOpen()){
            channel.close();
        }
    }

    boolean isKeepAliveThreadRunning() {
        try {
            keepAliveThreadExecutorLock.lock();
            return keepAliveThreadExecutor != null && !keepAliveThreadExecutor.isShutdown();
        } finally {
            keepAliveThreadExecutorLock.unlock();
        }
    }

    private void fetchBinlogFilenameAndPosition() throws IOException {
        ResultSetRowPacket[] resultSet;
        channel.write(new QueryCommand("show master status"));
        resultSet = readResultSet();
        if (resultSet.length == 0) {
            throw new IOException("Failed to determine binlog filename/position");
        }
        ResultSetRowPacket resultSetRow = resultSet[0];
        binlogFilename = resultSetRow.getValue(0);
        binlogPosition = Long.parseLong(resultSetRow.getValue(1));
        logger.log(Level.WARNING, "Binlog filename " + binlogFilename + " position " + binlogPosition);
    }


    private PacketChannel openChannel() throws IOException {
        Socket socket = new Socket();
        socket.connect(new InetSocketAddress(hostname, port), (int) connectTimeout);
        return new PacketChannel(socket);
    }

    private ResultSetRowPacket[] readResultSet() throws IOException {
        List<ResultSetRowPacket> resultSet = new LinkedList<ResultSetRowPacket>();
        byte[] statementResult = channel.read();
        checkError(statementResult);

        while ((channel.read())[0] != (byte) 0xFE /* eof */) { /* skip */}
        for (byte[] bytes; (bytes = channel.read())[0] != (byte) 0xFE; ) {
            resultSet.add(new ResultSetRowPacket(bytes));
        }

        return resultSet.toArray(new ResultSetRowPacket[resultSet.size()]);
    }

    private void checkError(byte[] packet) {

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

    private Callable scheduleDisconnectIn(final long timeout){
        final MySQLClient self = this;
        final CountDownLatch connectLatch = new CountDownLatch(1);
        final Thread thread = newNamedThread(new Runnable(){

            @Override
            public void run() {

            }
        },"");

        return new Callable() {
            @Override
            public Object call() throws Exception {
                return null;
            }
        };
    }

    private Thread newNamedThread(Runnable runnable, String threadName){
        Thread thread = threadFactory == null ? new Thread(runnable) : threadFactory.newThread(runnable);
        return thread;
    }
}
