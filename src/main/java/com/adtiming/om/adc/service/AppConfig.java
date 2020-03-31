// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.adc.service;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.net.InetSocketAddress;
import java.net.Proxy;

@Service
public class AppConfig {

    @Value("${app.env}")
    public String appEnv;

    @Value("${http.proxy}")
    public String httpProxyStr;

    public HttpHost httpProxy;
    public Proxy proxy;

    @Value("${auth.dir}")
    public String authDir;

    @Value("${auth.domain}")
    public String authDomain;

    @PostConstruct
    private void init() {
        if (StringUtils.isNotBlank(httpProxyStr)) {
            httpProxy = HttpHost.create(httpProxyStr);
            proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(httpProxy.getHostName(), httpProxy.getPort()));
        }
    }

    public boolean isProd() {
        return appEnv.equals("prod");
    }
}
