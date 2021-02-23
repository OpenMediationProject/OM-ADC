package com.adtiming.om.adc.service.ironsource;

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
public class RequestIronSource extends BaseTask {
    private static final Logger LOG = LogManager.getLogger();
    @Resource
    private AppConfig cfg;

    @Resource
    private JdbcTemplate jdbcTemplateW;

    private final int timeDelay = -1;

    @Scheduled(cron = "0 40 * * * ?")
    public void buildTask() {
        if (!cfg.isProd())
            return;

        LOG.info("[IronSource] buildCurrentTask Start");
        long start = System.currentTimeMillis();
        String[] days = { LocalDateTime.now().plusHours(timeDelay).format(DateTimeFormat.DAY_FORMAT) };
        rebuild(days);
        LOG.info("[IronSource] buildCurrentTask End, cost:{}", System.currentTimeMillis() - start);
    }

    @Scheduled(cron = "0 40 0 * * ?")
    public void buildResetTask() {
        if (!cfg.isProd())
            return;

        LOG.info("[IronSource] buildResetTask Start");
        long start = System.currentTimeMillis();
        String[] days = { LocalDateTime.now().plusHours(timeDelay).plusDays(-1).format(DateTimeFormat.DAY_FORMAT),
                LocalDateTime.now().plusHours(timeDelay).plusDays(-2).format(DateTimeFormat.DAY_FORMAT) };
        rebuild(days);
        LOG.info("[IronSource] buildResetTask End, cost:{}", System.currentTimeMillis() - start);
    }

    public void rebuild(String[] days) {
        try {
            buildTask(jdbcTemplateW, days, 0, 15, "IronSource");
        } catch (Exception e) {
            LOG.error("[IronSource] build task error", e);
        }
    }

    public void rebuild(String[] days, int id) {
        try {
            buildTaskById(jdbcTemplateW, id, days, 0, 15, "IronSource");
        } catch (Exception e) {
            LOG.error("[IronSource] build task error", e);
        }
    }
}
