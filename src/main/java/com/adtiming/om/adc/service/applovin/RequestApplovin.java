// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.adc.service.applovin;

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
public class RequestApplovin extends BaseTask {

    private static final Logger LOG = LogManager.getLogger();

    @Resource
    private AppConfig cfg;

    @Resource
    private JdbcTemplate jdbcTemplate;

    private final int TIME_DELAY = -8;

    @Scheduled(cron = "0 15 * * * ?")
    public void buildCurrentTask() {
        if (!cfg.isProd())
            return;
        LOG.info("[Applovin] buildCurrentTask Start");
        long start = System.currentTimeMillis();
        String[] days = { LocalDateTime.now().plusHours(TIME_DELAY).format(DateTimeFormat.DAY_FORMAT) };
        rebuild(days);
        LOG.info("[Applovin] buildCurrentTask End, cost:{}", System.currentTimeMillis() - start);
    }

    @Scheduled(cron = "0 20 0 * * ?")
    public void buildResetTask() {
        if (!cfg.isProd())
            return;
        LOG.info("[Applovin] buildResetTask Start");
        long start = System.currentTimeMillis();
        String[] days = { LocalDateTime.now().plusHours(TIME_DELAY).plusDays(-1).format(DateTimeFormat.DAY_FORMAT),
                LocalDateTime.now().plusDays(-2).format(DateTimeFormat.DAY_FORMAT) };
        rebuild(days);
        LOG.info("[Applovin] buildResetTask End, cost:{}", System.currentTimeMillis() - start);
    }

    public void rebuild(String[] days) {
        try {
            buildTask(jdbcTemplate, days, 0, 8, "Applovin");
        } catch (Exception e) {
            LOG.error("[Applovin] build task error", e);
        }
    }

    public void rebuild(String[] days, int accountId) {
        try {
            buildTaskById(jdbcTemplate, accountId, days, 0, 8, "Applovin");
        } catch (Exception e) {
            LOG.error("[Applovin] build task error", e);
        }
    }
}
