// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.adc.service.adtiming;

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
public class RequestAdtiming extends BaseTask {
    private static final Logger LOG = LogManager.getLogger();

    @Resource
    private AppConfig cfg;

    @Resource
    private JdbcTemplate jdbcTemplate;

    @Scheduled(cron = "0 30 * * * ?")
    public void buildCurrentTask() {
        if (!cfg.isProd())
            return;
        LOG.info("[Adtiming] buildCurrentTask Start");
        long start = System.currentTimeMillis();
        String[] days = { LocalDateTime.now().format(DateTimeFormat.DAY_FORMAT) };
        rebuild(days);
        LOG.info("[Adtiming] buildCurrentTask End, cost:{}", System.currentTimeMillis() - start);
    }

    @Scheduled(cron = "0 30 3 * * ?")
    public void rebuildTask() {
        if (!cfg.isProd())
            return;
        LOG.info("[Adtiming] buildResetTask Start");
        long start = System.currentTimeMillis();
        String[] days = { LocalDateTime.now().plusDays(-1).format(DateTimeFormat.DAY_FORMAT) };
        rebuild(days);
        LOG.info("[Adtiming] buildResetTask End, cost:{}", System.currentTimeMillis() - start);
    }

    public void rebuild(String[] days) {
        try {
            buildTask(jdbcTemplate, days, 0, 1, "Adtiming");
        } catch (Exception e) {
            LOG.error("[Adtiming] build task error", e);
        }
    }

    public void rebuild(String[] days, Integer accountId) {
        try {
            buildTaskById(jdbcTemplate, accountId, days, 0, 1, "Adtiming");
        } catch (Exception e) {
            LOG.error("[Adtiming] build task error", e);
        }
    }
}
