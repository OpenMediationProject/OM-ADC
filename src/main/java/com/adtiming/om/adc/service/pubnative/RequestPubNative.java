package com.adtiming.om.adc.service.pubnative;

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
public class RequestPubNative extends BaseTask {
    private static final Logger LOG = LogManager.getLogger();

    @Resource
    private AppConfig cfg;

    @Resource
    private JdbcTemplate jdbcTemplateW;

    private final int timeDelay = -10;

    @Scheduled(cron = "0 33 * * * ?")
    public void run() {
        if (!cfg.isProd())
            return;

        LOG.info("[PubNative] buildCurrentTask Start");
        long start = System.currentTimeMillis();
        String[] days = { LocalDateTime.now().plusHours(timeDelay).format(DateTimeFormat.DAY_FORMAT) };
        rebuild(days);
        LOG.info("[PubNative] buildCurrentTask End, cost:{}", System.currentTimeMillis() - start);
    }

    @Scheduled(cron = "0 33 0 * * ?")
    public void yesterday_run() {
        if (!cfg.isProd())
            return;

        LOG.info("[PubNative] buildResetTask Start");
        long start = System.currentTimeMillis();
        String[] days = {
                LocalDateTime.now().plusHours(timeDelay).plusDays(-1).format(DateTimeFormat.DAY_FORMAT),
                LocalDateTime.now().plusHours(timeDelay).plusDays(-2).format(DateTimeFormat.DAY_FORMAT)
        };
        rebuild(days);
        LOG.info("[PubNative] buildResetTask End, cost:{}", System.currentTimeMillis() - start);
    }

    public void rebuild(String[] days) {
        try {
            buildTask(jdbcTemplateW, days, 0, 23, "PubNative");
        } catch (Exception e) {
            LOG.error("[PubNative] build task error", e);
        }
    }

    public void rebuild(String[] days, int id) {
        try {
            buildTaskById(jdbcTemplateW, id, days, 0, 23, "PubNative");
        } catch (Exception e) {
            LOG.error("[PubNative] build task error", e);
        }
    }
}
