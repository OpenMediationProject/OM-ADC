// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.adc.util;


import org.apache.http.client.CookieStore;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.config.SocketConfig;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.cookie.Cookie;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.protocol.HttpContext;
import org.apache.http.ssl.SSLContextBuilder;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.net.Proxy;
import java.net.Socket;

public class MyHttpClient {

    private MyHttpClient() {
    }

    /**
     * 添加全局cookie
     *
     * @param cookie
     */
    public static void addCookie(Cookie cookie) {
        COOKIE_STORE.addCookie(cookie);
    }

    public static CookieStore getCookieStore() {
        return COOKIE_STORE;
    }

    private static final BasicCookieStore COOKIE_STORE;
    private static final CloseableHttpClient client;
    protected static final SSLContext sslContext;

    static {
        PoolingHttpClientConnectionManager cm;
        try {
            sslContext = SSLContextBuilder.create().loadTrustMaterial(null, (TrustStrategy) (chain, authType) -> {
                return true;// 信任所有
            }).build();

            HttpsURLConnection.setDefaultSSLSocketFactory(sslContext.getSocketFactory());
            Registry<ConnectionSocketFactory> r = RegistryBuilder.<ConnectionSocketFactory>create()
                    .register("http", new PlainConnectionSocketFactory() {
                        public Socket createSocket(final HttpContext context) throws IOException {
                            return do_createSocket(context);
                        }
                    }).register("https", new SSLConnectionSocketFactory(sslContext) {
                        public Socket createSocket(final HttpContext context) throws IOException {
                            return do_createSocket(context);
                        }
                    }).build();
            cm = new PoolingHttpClientConnectionManager(r);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        HttpClientBuilder builder = HttpClientBuilder.create();
        cm.setDefaultMaxPerRoute(50000);
        cm.setMaxTotal(10000000);

        ConnectionConfig.Builder ccb = ConnectionConfig.copy(ConnectionConfig.DEFAULT);
        ccb.setBufferSize(8192);
        cm.setDefaultConnectionConfig(ccb.build());

        SocketConfig.Builder scb = SocketConfig.copy(SocketConfig.DEFAULT);
        scb.setRcvBufSize(8192);
        scb.setSndBufSize(8192);
        scb.setSoTimeout(5000);
        cm.setDefaultSocketConfig(scb.build());

        builder.setUserAgent("Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.1.5)");

        builder.setConnectionManager(cm);

        RequestConfig req_cfg = RequestConfig.custom().setConnectTimeout(5000).build();
        builder.setDefaultRequestConfig(req_cfg);

        COOKIE_STORE = new BasicCookieStore();
        builder.setDefaultCookieStore(COOKIE_STORE);

        client = builder.build();

    }

    private static Socket do_createSocket(final HttpContext context) {
        Socket sc;
        Proxy socks_proxy = (Proxy) context.getAttribute("socks.proxy");
        if (socks_proxy == null)
            sc = new Socket();
        else
            sc = new Socket(socks_proxy);
        return sc;
    }

    public static HttpClient getInstance() {
        return client;
    }

}
