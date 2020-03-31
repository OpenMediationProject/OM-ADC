// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.adc.util;


import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.HttpConnectionFactory;
import org.apache.http.conn.ManagedHttpClientConnection;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.DefaultHttpResponseParserFactory;
import org.apache.http.impl.conn.ManagedHttpClientConnectionFactory;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.impl.io.DefaultHttpRequestWriterFactory;
import org.apache.http.io.HttpMessageParserFactory;
import org.apache.http.io.HttpMessageWriterFactory;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.util.EntityUtils;

import javax.net.ssl.SSLContext;
import java.io.IOException;

public class Http {
    private static SSLContext sslcontext = SSLContexts.createSystemDefault();
    private static HttpMessageParserFactory<HttpResponse> responseParserFactory = new DefaultHttpResponseParserFactory();
    private static HttpMessageWriterFactory<HttpRequest> requestWriterFactory = new DefaultHttpRequestWriterFactory();
    private static PoolingHttpClientConnectionManager connManager;
    private static RequestConfig defaultRequestConfig;

    public Http() {
    }

    public static String post(String url, int timeout, HttpHost http_proxy) throws IOException {
        return execute(new HttpPost(url), timeout, http_proxy);
    }

    public static <T> T postJson(String url, Class<T> classOfT, int timeout, HttpHost http_proxy) throws IOException {
        String json = post(url, timeout, http_proxy);
        if (json != null) {
            T t = JsonHelper.readObject(json, classOfT);
            if (t != null) {
                return t;
            }
        }
        return null;
    }

    public static <T> T getJson(String url, Class<T> classOfT, int timeout, HttpHost http_proxy) throws IOException {
        String json = get(url, timeout, http_proxy);
        if (json != null) {
            T t = JsonHelper.readObject(json, classOfT);
            if (t != null) {
                return t;
            }
        }
        return null;
    }

    public static String get(String url, int timeout, HttpHost http_proxy) throws IOException {
        return execute(new HttpGet(url), timeout, http_proxy);
    }

    private static String execute(HttpRequestBase req, int timeout, HttpHost proxy) throws IOException {
        String re = null;
        RequestConfig requestConfig = RequestConfig.copy(defaultRequestConfig).setProxy(proxy).setConnectTimeout(timeout).build();
        req.setConfig(requestConfig);
        CloseableHttpClient client;
        client = HttpClients.custom().setConnectionManager(connManager).setDefaultRequestConfig(defaultRequestConfig).build();
        CloseableHttpResponse response = client.execute(req);

        try {
            HttpEntity entity = response.getEntity();
            if (entity != null) {
                re = EntityUtils.toString(entity, "UTF-8");
                EntityUtils.consume(entity);
            }
        } finally {
            if (response != null) {
                response.close();
            }

        }
        return re;
    }

    static {
        try {
            Registry socketFactoryRegistry = RegistryBuilder.create().register("http", PlainConnectionSocketFactory.INSTANCE).register("https", new SSLConnectionSocketFactory(sslcontext)).build();
            HttpConnectionFactory<HttpRoute, ManagedHttpClientConnection> connFactory = new ManagedHttpClientConnectionFactory(requestWriterFactory, responseParserFactory);
            connManager = new PoolingHttpClientConnectionManager(socketFactoryRegistry, connFactory);
            defaultRequestConfig = RequestConfig.custom().setCookieSpec("default").setExpectContinueEnabled(true).setConnectTimeout(6000).build();
            connManager.setMaxTotal(20);
        } catch (Exception ignored){
        }
    }
}