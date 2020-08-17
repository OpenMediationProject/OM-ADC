package com.adtiming.om.adc.dto;

import org.apache.commons.lang3.StringUtils;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.RowMapper;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by huangqiang on 2020/7/27.
 * ReportApiError
 */
public class ReportApiError {
    public static final RowMapper<ReportApiError> ROWMAPPER = BeanPropertyRowMapper.newInstance(ReportApiError.class);
    public int id;
    public int adnId;
    public String errorCode;
    public String reason;
    public String content;
    public String solution;
    public String solutionCn;
    public int isIgnore;

    public Set<String> errMsg = new HashSet<>();

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

    public String getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(String errorCode) {
        this.errorCode = errorCode;
    }

    public String getReason() {
        return reason;
    }

    public void setReason(String reason) {
        if (StringUtils.isNoneBlank(reason)) {
            errMsg.addAll(Arrays.stream(reason.split("[\r\n]")).filter(StringUtils::isNotBlank).collect(Collectors.toList()));
        }
        this.reason = reason;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String getSolution() {
        return solution;
    }

    public void setSolution(String solution) {
        this.solution = solution;
    }

    public String getSolutionCn() {
        return solutionCn;
    }

    public void setSolutionCn(String solutionCn) {
        this.solutionCn = solutionCn;
    }

    public int getIsIgnore() {
        return isIgnore;
    }

    public void setIsIgnore(int isIgnore) {
        this.isIgnore = isIgnore;
    }

    // Ignore error
    public boolean isIgnore() {
        return this.isIgnore == 1;
    }
}
