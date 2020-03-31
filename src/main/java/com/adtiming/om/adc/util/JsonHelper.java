// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.adc.util;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.util.EntityUtils;

import java.io.IOException;

public abstract class JsonHelper {

    private JsonHelper() {
    }

    private static final ObjectMapper objectMapper;

    public static ObjectMapper getObjectMapper() {
        return objectMapper;
    }

    static {
        objectMapper = new ObjectMapper();
        objectMapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
        objectMapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);
        objectMapper.configure(JsonGenerator.Feature.IGNORE_UNKNOWN, true);
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    }

    public static <T> T readObject(String json, Class<T> ctype) throws IOException {
        return objectMapper.readValue(json, ctype);
    }

    public static Object readObject(String json) throws IOException {
        return objectMapper.readValue(json, Object.class);
    }

    public static <T> T execute(String uri, JavaType type, int soTimeout) {
        return execute(new HttpGet(uri), type, soTimeout);
    }

    public static <T> T execute(HttpRequestBase req, JavaType type, int soTimeout) {
        HttpEntity entity = null;
        T t = null;
        try {
            req.setConfig(RequestConfig.custom().setSocketTimeout(soTimeout).build());
            HttpResponse resp = MyHttpClient.getInstance().execute(req);
            entity = resp.getEntity();
            if (entity != null) {
                t = objectMapper.readValue(ParserHelper.getContentReader(entity), type);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            EntityUtils.consumeQuietly(entity);
        }
        return t;
    }

    public static <T> T execute(String uri, Class<T> type, int soTimeout) {
        return execute(new HttpGet(uri), type, soTimeout);
    }

    public static <T> T execute(HttpRequestBase req, Class<T> type, int soTimeout) {
        HttpEntity entity = null;
        T t = null;
        try {
            req.setConfig(RequestConfig.custom().setSocketTimeout(soTimeout).build());
            HttpResponse resp = MyHttpClient.getInstance().execute(req);
            entity = resp.getEntity();
            if (entity != null) {
                t = objectMapper.readValue(ParserHelper.getContentReader(entity), type);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            EntityUtils.consumeQuietly(entity);
        }
        return t;
    }

    /**
     * convert object to string as json format
     *
     * @param o
     * @return string as json format
     */
    public static String toJson(Object o) {
        try {
            return JsonHelper.getObjectMapper().writeValueAsString(o);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}