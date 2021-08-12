package com.adtiming.om.adc.util;

import com.alibaba.fastjson.JSONObject;
import com.google.api.services.admob.v1.model.Date;
import io.micrometer.core.instrument.util.StringUtils;

import java.math.BigDecimal;
import java.time.ZonedDateTime;

public class Util {

    public static String getJSONString(JSONObject obj, String key) {
        String val = obj.getString(key);
        return StringUtils.isNotBlank(val) ? val : "";
    }

    public static BigDecimal getJSONDecimal(JSONObject obj, String key) {
        BigDecimal val = obj.getBigDecimal(key);
        return val != null ? val : BigDecimal.ZERO;
    }

    public static int getJSONInt(JSONObject obj, String key) {
        Integer val = obj.getInteger(key);
        return val != null ? val : 0;
    }

    public static Date toDate(ZonedDateTime dateTime) {
        return new Date()
                .setYear(dateTime.getYear())
                .setMonth(dateTime.getMonthValue())
                .setDay(dateTime.getDayOfMonth());
    }
}
