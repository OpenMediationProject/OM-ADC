// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.adc.service.vungle;

import com.adtiming.om.adc.service.AppConfig;
import com.adtiming.om.adc.service.BaseTask;
import com.adtiming.om.adc.util.DateTimeFormat;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.time.LocalDateTime;

@Service
public class RequestVungle extends BaseTask {

    private static final Logger LOG = LogManager.getLogger();

    @Resource
    private AppConfig cfg;

    @Resource
    private JdbcTemplate jdbcTemplate;

    private final int TIME_DELAY = -8;

    @Scheduled(cron = "0 45 * * * ?")
    public void buildCurrentTask() {
        if (!cfg.isProd())
            return;

        LOG.info("[Vungle] buildCurrentTask Start");
        long start = System.currentTimeMillis();
        String[] days = { LocalDateTime.now().plusHours(TIME_DELAY).format(DateTimeFormat.DAY_FORMAT) };
        rebuild(days);
        LOG.info("[Vungle] buildCurrentTask End, cost:{}", System.currentTimeMillis() - start);
    }

    @Scheduled(cron = "0 50 0 * * ?")
    public void buildYesterdayTask() {
        if (!cfg.isProd())
            return;

        LOG.info("[Vungle] buildYesterdayTask Start");
        long start = System.currentTimeMillis();
        String[] days = { LocalDateTime.now().plusHours(TIME_DELAY).plusDays(-1).format(DateTimeFormat.DAY_FORMAT) };
        rebuild(days);
        LOG.info("[Vungle] buildYesterdayTask End, cost:{}", System.currentTimeMillis() - start);
    }

    public void rebuild(String[] days) {
        try {
            buildTask(jdbcTemplate, days, 0, 5, "Vungle");
        } catch (Exception e) {
            LOG.error("[Vungle] build task error", e);
        }
    }

    public void rebuild(String[] days, int id) {
        try {
            buildTaskById(jdbcTemplate, id, days, 0, 5, "Vungle");
        } catch (Exception e) {
            LOG.error("[Vungle] build task error", e);
        }
    }
}
