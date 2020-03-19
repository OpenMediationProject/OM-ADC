// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.adc.dto;

import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.RowMapper;

public class ReportAccount {
    public static final RowMapper<ReportAccount> ROWMAPPER = BeanPropertyRowMapper.newInstance(ReportAccount.class);
    public int id;
    public int adnId;
    public int adnAccountId;
    public String adnAppId;
    public String adnApiKey;
    public String adnAppToken;
    public String userId;
    public String userSignature;
    public String credentialPath;
    public String authKey; // admob证书重定向URL中唯一主键

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getAdnId() {
        return adnId;
    }

    public void setAdnId(int adnId) {
        this.adnId = adnId;
    }

    public int getAdnAccountId() {
        return adnAccountId;
    }

    public void setAdnAccountId(int adnAccountId) {
        this.adnAccountId = adnAccountId;
    }

    public String getAdnAppId() {
        return adnAppId;
    }

    public void setAdnAppId(String adnAppId) {
        this.adnAppId = adnAppId;
    }

    public String getAdnApiKey() {
        return adnApiKey;
    }

    public void setAdnApiKey(String adnApiKey) {
        this.adnApiKey = adnApiKey;
    }

    public String getAdnAppToken() {
        return adnAppToken;
    }

    public void setAdnAppToken(String adnAppToken) {
        this.adnAppToken = adnAppToken;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getUserSignature() {
        return userSignature;
    }

    public void setUserSignature(String userSignature) {
        this.userSignature = userSignature;
    }

    public String getCredentialPath() {
        return credentialPath;
    }

    public void setCredentialPath(String credentialPath) {
        this.credentialPath = credentialPath;
    }

    public String getAuthKey() {
        return authKey;
    }

    public void setAuthKey(String authKey) {
        this.authKey = authKey;
    }
}
