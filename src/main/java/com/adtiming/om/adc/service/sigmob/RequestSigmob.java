// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.adc.service.sigmob;

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
public class RequestSigmob extends BaseTask {
    private static final Logger LOG = LogManager.getLogger();

    @Resource
    private AppConfig cfg;

    @Resource
    private JdbcTemplate jdbcTemplateW;

    //每日12点后，可查询前一天的数据；最长维度为“90天”
    @Scheduled(cron = "0 33 14 * * ?")
    public void run() {
        if (!cfg.isProd())
            return;

        LOG.info("[Sigmob] buildCurrentTask Start");
        long start = System.currentTimeMillis();
        String[] days = { LocalDateTime.now().plusDays(-1).format(DateTimeFormat.DAY_FORMAT) };
        rebuild(days);
        LOG.info("[Sigmob] buildCurrentTask End, cost:{}", System.currentTimeMillis() - start);
    }

    @Scheduled(cron = "0 33 0 * * ?")
    public void yesterday_run() {
        if (!cfg.isProd())
            return;

        LOG.info("[Sigmob] buildResetTask Start");
        long start = System.currentTimeMillis();
        String[] days = {
                LocalDateTime.now().plusDays(-2).format(DateTimeFormat.DAY_FORMAT),
                LocalDateTime.now().plusDays(-3).format(DateTimeFormat.DAY_FORMAT)
        };
        rebuild(days);
        LOG.info("[Sigmob] buildResetTask End, cost:{}", System.currentTimeMillis() - start);
    }

    public void rebuild(String[] days) {
        try {
            buildTask(jdbcTemplateW, days, 0, 20, "Sigmob");
        } catch (Exception e) {
            LOG.error("[Sigmob] build task error", e);
        }
    }

    public void rebuild(String[] days, int id) {
        try {
            buildTaskById(jdbcTemplateW, id, days, 0, 20, "Sigmob");
        } catch (Exception e) {
            LOG.error("[Sigmob] build task error", e);
        }
    }
}
