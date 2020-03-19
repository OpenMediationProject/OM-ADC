// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.adc.dto;

import com.adtiming.om.adc.util.JsonHelper;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.LinkedHashMap;
import java.util.Map;

@JsonIgnoreProperties({"cause", "stackTrace", "message", "localizedMessage", "suppressed"})
public class Response extends RuntimeException {

    public static final Response RES_UNAUTHORIZED = new Response(403, "unauthorized", "权限不足");

    public int code;
    public String status;
    public String msg;
    public Map<String, Object> data;

    public Response() {
    }

    public Response(Map<String, Object> data) {
        this.data = data;
    }

    public Response(int code, String status, String msg) {
        this.code = code;
        this.status = status;
        this.msg = msg;
    }

    public static Response build(int code, String status, String msg) {
        return new Response(code, status, msg);
    }

    public static Response build() {
        return new Response(0, "", "OK");
    }

    public Response code(int code) {
        this.code = code;
        return this;
    }

    public Response stats(String stats) {
        this.status = stats;
        return this;
    }

    public Response msg(String msg) {
        this.msg = msg;
        return this;
    }

    public Response setData(Map<String, Object> data) {
        this.data = data;
        return this;
    }

    /**
     * 添加扩展数据, 保存在 data 数据中, map 结构
     */
    public Response data(String key, Object v) {
        if (data == null)
            data = new LinkedHashMap<>();
        data.put(key, v);
        return this;
    }

    public Object data(String key) {
        return data == null ? null : data.get(key);
    }

    public Object removeData(String key) {
        return data == null ? null : data.remove(key);
    }

    @Override
    public String toString() {
        return JsonHelper.toJson(this);
    }

}

