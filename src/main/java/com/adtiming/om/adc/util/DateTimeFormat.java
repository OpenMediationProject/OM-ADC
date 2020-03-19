// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.adc.util;

import java.time.format.DateTimeFormatter;

/**
 * Created by wangqf on 2017/12/20.
 */
public class DateTimeFormat {
    public static final DateTimeFormatter TIME_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    public static final DateTimeFormatter DAY_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    public static final DateTimeFormatter DAY_O_FORMAT = DateTimeFormatter.ofPattern("MMddyyyy");
    public static final DateTimeFormatter HOUR_FORMAT = DateTimeFormatter.ofPattern("HH");
    public static final DateTimeFormatter ISO_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'+'SSSS");
}
