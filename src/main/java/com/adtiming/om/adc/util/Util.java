package com.adtiming.om.adc.util;

import com.alibaba.fastjson.JSONObject;
import io.micrometer.core.instrument.util.StringUtils;

import java.math.BigDecimal;

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
}
