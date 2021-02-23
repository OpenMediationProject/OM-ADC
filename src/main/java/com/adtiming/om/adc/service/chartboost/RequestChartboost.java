// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.adc.service.chartboost;

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
public class RequestChartboost extends BaseTask {

    private static final Logger LOG = LogManager.getLogger();

    @Resource
    private AppConfig cfg;

    @Resource
    private JdbcTemplate jdbcTemplate;

    private final int TIME_DELAY = -12;

    @Scheduled(cron = "0 18 * * * ?")
    public void buildTodayTask() {
        if (!cfg.isProd())
            return;

        LOG.info("[Chartboost] build today task Start");
        long start = System.currentTimeMillis();
        String[] days = {LocalDateTime.now().plusHours(TIME_DELAY).format(DateTimeFormat.DAY_FORMAT)};
        rebuild(days);
        LOG.info("[Chartboost] build today task End, cost:{}", System.currentTimeMillis() - start);
    }

    @Scheduled(cron = "0 40 0 * * ?")
    public void buildYesterdayTask() {
        if (!cfg.isProd())
            return;

        LOG.info("[Chartboost] build yesterday task Start");
        long start = System.currentTimeMillis();
        String[] days = {LocalDateTime.now().plusHours(TIME_DELAY).plusDays(-1).format(DateTimeFormat.DAY_FORMAT)};
        rebuild(days);
        LOG.info("[Chartboost] build yesterday task End, cost:{}", System.currentTimeMillis() - start);
    }

    public void rebuild(String[] days) {
        try {
            buildTask(jdbcTemplate, days, 0, 12, "Chartboost");
        } catch (Exception e) {
            LOG.error("[Applovin] build task error", e);
        }
    }

    public void rebuild(String[] days, int accountId) {
        try {
            buildTaskById(jdbcTemplate, accountId, days, 0, 12, "Chartboost");
        } catch (Exception e) {
            LOG.error("[Applovin] build task error", e);
        }
    }
}
