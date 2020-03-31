// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.adc.service.unity;

import com.adtiming.om.adc.service.AppConfig;
import com.adtiming.om.adc.service.BaseTask;
import com.adtiming.om.adc.util.DateTimeFormat;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.time.LocalDateTime;

@Service
public class RequestUnity extends BaseTask {

    private static final Logger LOG = LogManager.getLogger();

    @Resource
    private AppConfig cfg;

    @Resource
    private JdbcTemplate jdbcTemplate;

    private final int TIME_DELAY = -10;

    @Scheduled(cron = "0 50 * * * *")
    public void buildCurrentTask() {
        if (!cfg.isProd())
            return;

        LocalDateTime date = LocalDateTime.now().plusHours(TIME_DELAY);
        String[] day = new String[] {LocalDateTime.now().plusHours(TIME_DELAY).format(DateTimeFormat.DAY_FORMAT)};
        int hour = NumberUtils.toInt(DateTimeFormat.HOUR_FORMAT.format(date));
        LOG.info("[Unity] buildCurrentTask Run Start, day:{},hour:{}", day, hour);
        long start = System.currentTimeMillis();
        rebuild(day, hour);
        LOG.info("[Unity] buildCurrentTask Run End, day:{},hour:{},cost;{}", day, hour, System.currentTimeMillis() - start);
    }

    @Scheduled(cron = "0 20 1 * * ?")
    public void buildYesterdayTask() {
        if (!cfg.isProd())
            return;

        LOG.info("[Unity] buildYesterdayTask Start");
        long start = System.currentTimeMillis();
        String[] day = new String[] {LocalDateTime.now().plusHours(TIME_DELAY).plusDays(-1).format(DateTimeFormat.DAY_FORMAT)};
        for (int i = 0; i < 24; i++) {
            rebuild(day, i);
        }
        LOG.info("[Unity] buildYesterdayTask End, cost:{}", System.currentTimeMillis() - start);
    }

    public void rebuild(String[] day, int hour) {
        try {
            buildTask(jdbcTemplate, day, hour, 4, "Unity");
        } catch (Exception e) {
            LOG.error("[Unity] build task error", e);
        }
    }

    public void rebuild(String[] day, int hour, int id) {
        try {
            buildTaskById(jdbcTemplate, id, day, hour, 4, "Unity");
        } catch (Exception e) {
            LOG.error("[Unity] build task error", e);
        }
    }
}
