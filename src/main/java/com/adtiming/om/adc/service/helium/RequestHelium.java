package com.adtiming.om.adc.service.helium;

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
public class RequestHelium extends BaseTask {

    private static final Logger LOG = LogManager.getLogger();

    @Resource
    private AppConfig cfg;

    @Resource
    private JdbcTemplate jdbcTemplateW;

    private final int timeDelay = -20;

    @Scheduled(cron = "0 18 * * * ?")
    public void buildTodayTask() {
        if (!cfg.isProd())
            return;

        LOG.info("[Helium] build today task Start");
        long start = System.currentTimeMillis();
        String[] days = {LocalDateTime.now().plusHours(timeDelay).format(DateTimeFormat.DAY_FORMAT)};
        rebuild(days);
        LOG.info("[Helium] build today task End, cost:{}", System.currentTimeMillis() - start);
    }

    @Scheduled(cron = "0 23 00 * * ?")
    public void buildYesterdayTask() {
        if (!cfg.isProd())
            return;

        LOG.info("[Helium] build yesterday task Start");
        long start = System.currentTimeMillis();
        String[] days = {LocalDateTime.now().plusHours(timeDelay).plusDays(-1).format(DateTimeFormat.DAY_FORMAT)};
        rebuild(days);
        LOG.info("[Helium] build yesterday task End, cost:{}", System.currentTimeMillis() - start);
    }

    public void rebuild(String[] days) {
        try {
            buildTask(jdbcTemplateW, days, 0, 17, "Helium");
        } catch (Exception e) {
            LOG.error("[Helium] build task error", e);
        }
    }

    public void rebuild(String[] days, int id) {
        try {
            buildTaskById(jdbcTemplateW, id, days, 0, 17, "Helium");
        } catch (Exception e) {
            LOG.error("[Helium] build task error", e);
        }
    }
}
