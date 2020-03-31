// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.adc.dto;

import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.RowMapper;

public class ReportTask {
    public static final RowMapper<ReportTask> ROWMAPPER = BeanPropertyRowMapper.newInstance(ReportTask.class);
    public int id;
    public String day;
    public int hour;
    public int adnId;
    public String queryId;
    public String adnAppId;
    public String adnApiKey;
    public String adnAppToken;
    public String userId;
    public String userSignature;
    public String reqUrl;
    public int runCount;
    public int status;
    public String credentialPath;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getDay() {
        return day;
    }

    public void setDay(String day) {
        this.day = day;
    }

    public int getHour() {
        return hour;
    }

    public void setHour(int hour) {
        this.hour = hour;
    }

    public int getAdnId() {
        return adnId;
    }

    public void setAdnId(int adnId) {
        this.adnId = adnId;
    }

    public String getQueryId() {
        return queryId;
    }

    public void setQueryId(String queryId) {
        this.queryId = queryId;
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

    public String getReqUrl() {
        return reqUrl;
    }

    public void setReqUrl(String reqUrl) {
        this.reqUrl = reqUrl;
    }

    public int getRunCount() {
        return runCount;
    }

    public void setRunCount(int runCount) {
        this.runCount = runCount;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public String getCredentialPath() {
        return credentialPath;
    }

    public void setCredentialPath(String credentialPath) {
        this.credentialPath = credentialPath;
    }
}
