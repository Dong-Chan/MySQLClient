package com.github.dongchan;

import com.github.dongchan.network.protocol.PacketChannel;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * @author DongChan
 * @date 2020/11/16
 * @time 5:29 PM
 */
public class MySQLClientTest {

    private MySQLClient client;
    private PacketChannel channel;

    private String localHostname = "127.0.0.1";
    private int port = 33081;
    private String localUsername = "super";
    private String localPassword = "super";

    @Before
    public void setUp() throws IOException {
        client = new MySQLClient(localHostname, port, null, localUsername, localPassword);
    }

    @Test
    public void fetchBinlogFilenameAndPosition() throws IOException {
//        client.connect();
       client.connectAndFetchStatus();
    }

}
