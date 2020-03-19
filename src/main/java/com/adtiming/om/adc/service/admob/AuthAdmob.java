// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.adc.service.admob;

import com.google.api.client.auth.oauth2.AuthorizationCodeRequestUrl;
import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.auth.oauth2.TokenResponse;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.api.services.adsense.AdSense;
import com.google.api.services.adsense.AdSenseScopes;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.net.Proxy;
import java.util.Collections;

public class AuthAdmob {

    private static final Logger LOG = LogManager.getLogger();
    // Global instance of the JSON factory.
    private final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
    private FileDataStoreFactory DATA_STORE_FACTORY;
    private NetHttpTransport httpTransport;
    private static final String APPLICATION_NAME = "google_api";
    private Proxy proxy;
    //private ReportAccount account;
    private String adn_app_token;
    private String adn_api_key;
    private String auth_domain;

    public AuthAdmob(Proxy proxy, String auth_dir, String adn_api_key, String adn_app_token, String credential_path, String auth_domain) {
        this.proxy = proxy;
        this.adn_api_key = adn_api_key;
        this.adn_app_token = adn_app_token;
        this.auth_domain = auth_domain;
        File cf = new File(auth_dir + credential_path);
        if (!cf.exists()) {
            cf.mkdirs();
        }
        try {
            httpTransport = new NetHttpTransport.Builder().setProxy(proxy).build();
            DATA_STORE_FACTORY = new FileDataStoreFactory(cf);
        } catch (Exception e) {
            LOG.error("init AuthAdmob error", e);
        }
    }

    public String getUrl(String userId) throws Exception {
        try {
            GoogleAuthorizationCodeFlow flow = this.getFlow();

            AuthorizationCodeRequestUrl authorizationUrl = flow.newAuthorizationUrl().setRedirectUri(getRedirectUrl(userId));
            return authorizationUrl.build();
        } catch (Exception e) {
            LOG.error("AuthAdmob getUrl error", e.getMessage());
            throw new Exception(e);
        }
    }

    private GoogleAuthorizationCodeFlow getFlow() throws IOException {
        // load client secrets
        GoogleClientSecrets clientSecrets = GoogleClientSecrets.load(JSON_FACTORY,
                new StringReader(adn_app_token));
        if (clientSecrets.getDetails().getClientId().startsWith("Enter")
                || clientSecrets.getDetails().getClientSecret().startsWith("Enter ")) {
            throw new IOException("client " + adn_api_key + " not found");
        }
        GoogleAuthorizationCodeFlow flow = new GoogleAuthorizationCodeFlow.Builder(
                httpTransport, JSON_FACTORY, clientSecrets,
                Collections.singletonList(AdSenseScopes.ADSENSE_READONLY))
                .setDataStoreFactory(DATA_STORE_FACTORY)
                .setApprovalPrompt("force")
                .setAccessType("offline")
                .build();
        return flow;
    }

    private String getRedirectUrl(String authKey) {
        //回调地址
        String url = "http://%s/report/callback/admob/%s";
        return String.format(url, auth_domain, authKey);
    }

    /**
     * 获取用户的身份凭据
     *
     * @return 如果为空，说明用户还没有凭据
     * @throws IOException
     */
    public AdSense getAdSense() throws IOException {
        Credential credential = getCredential();
        if (credential != null) {
            LOG.debug("refresh:" + credential.getRefreshToken());
            LOG.debug("access:" + credential.getAccessToken());
            return new AdSense.Builder(
                    new NetHttpTransport.Builder().setProxy(proxy).build(), JSON_FACTORY, credential).setApplicationName(APPLICATION_NAME)
                    .build();
        }
        return null;
    }

    private Credential getCredential() throws IOException {
        // load client secrets
        GoogleClientSecrets clientSecrets = GoogleClientSecrets.load(JSON_FACTORY,
                new StringReader(adn_app_token));
        if (clientSecrets.getDetails().getClientId().startsWith("Enter")
                || clientSecrets.getDetails().getClientSecret().startsWith("Enter ")) {
            return null;
        }
        // authorize

        // set up authorization code flow
        // set up authorization code flow
        GoogleAuthorizationCodeFlow flow = getFlow();
        Credential credential = flow.loadCredential(adn_api_key);
        if (credential != null) {
            if (credential.getRefreshToken() != null && credential.getExpiresInSeconds() < 60L) {
                if (credential.refreshToken()) {
                    return credential;
                }
            }
            return credential;
        }
        return null;
    }

    public void callback(String authKey, String code, String error) throws Exception {
        try {
            TokenResponse response = this.getFlow().newTokenRequest(code).setRedirectUri(this.getRedirectUrl(authKey)).execute();
            this.getFlow().createAndStoreCredential(response, adn_api_key);
        } catch (Exception e) {
            LOG.error("Admob Report callback error", e);
            //throw new Exception(e);
        }
    }
}
