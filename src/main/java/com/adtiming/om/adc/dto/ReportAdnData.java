// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.adc.dto;

import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.RowMapper;

import java.math.BigDecimal;

public class ReportAdnData {
    public static final RowMapper<ReportAdnData> ROWMAPPER = BeanPropertyRowMapper.newInstance(ReportAdnData.class);
    public String day;
    public int hour = 0;
    public String country;
    public BigDecimal cost = BigDecimal.ZERO;
    public BigDecimal revenue = BigDecimal.ZERO;
    public long apiRequest = 0L;
    public long apiFilled = 0L;
    public long apiImpr = 0L;
    public long apiClick = 0L;
    public long apiVideoStart = 0L;
    public long apiVideoComplete = 0L;
    public String dataKey;

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

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public BigDecimal getCost() {
        return cost;
    }

    public void setCost(BigDecimal cost) {
        this.cost = cost;
    }

    public BigDecimal getRevenue() {
        return revenue;
    }

    public void setRevenue(BigDecimal revenue) {
        this.revenue = revenue;
    }

    public long getApiRequest() {
        return apiRequest;
    }

    public void setApiRequest(long apiRequest) {
        this.apiRequest = apiRequest;
    }

    public long getApiFilled() {
        return apiFilled;
    }

    public void setApiFilled(long apiFilled) {
        this.apiFilled = apiFilled;
    }

    public long getApiClick() {
        return apiClick;
    }

    public void setApiClick(long apiClick) {
        this.apiClick = apiClick;
    }

    public long getApiImpr() {
        return apiImpr;
    }

    public void setApiImpr(long apiImpr) {
        this.apiImpr = apiImpr;
    }

    public long getApiVideoStart() {
        return apiVideoStart;
    }

    public void setApiVideoStart(long apiVideoStart) {
        this.apiVideoStart = apiVideoStart;
    }

    public long getApiVideoComplete() {
        return apiVideoComplete;
    }

    public void setApiVideoComplete(long apiVideoComplete) {
        this.apiVideoComplete = apiVideoComplete;
    }

    public String getDataKey() {
        return dataKey;
    }

    public void setDataKey(String dataKey) {
        this.dataKey = dataKey;
    }
}
