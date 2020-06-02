// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.adc.service;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;

@Service
public class ReportAccountService {
    private static final Logger LOG = LogManager.getLogger();

    @Resource
    private JdbcTemplate jdbcTemplateW;

    @Scheduled(cron = "0 0 * * * *")
    public void pauseExceptionAccount() {
        //Pause the data pull account that failed 10 times
        LOG.info("pauseExceptionAccount start");
        long start = System.currentTimeMillis();
        int count = 0;
        try {
            List<Integer> list = jdbcTemplateW.queryForList("SELECT report_account_id FROM report_adnetwork_task WHERE status=3 AND run_count>10", Integer.class);
            count = list.size();
            if (!list.isEmpty()) {
                LOG.error("Account paused, accountIds:{}", list);
                jdbcTemplateW.update(String.format("update report_adnetwork_account set status=0 where id in (%s)", StringUtils.join(list, ",")));
            }
        } catch (Exception e) {
            LOG.error("pauseExceptionAccount failed", e);
        }
        LOG.info("pauseExceptionAccount end, pauseCount:{}, cost:{}", count, System.currentTimeMillis() - start);
    }

    @Scheduled(cron = "0 0 1 * * *")
    public void deletePastOldReportAccount() {
        int count = 0;
        LOG.info("deletePastOldReportAccount start");
        int updateCount = 0;
        long start = System.currentTimeMillis();
        while (count < 5) {
            try {
                //delete the changed configuration 7 days ago
                String updateSql = "UPDATE om_adnetwork_app_change SET status=3 WHERE create_time<DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 7 DAY)";
                updateCount = jdbcTemplateW.update(updateSql);
                break;
            } catch (Exception e) {
                count++;
                if (count == 5) {
                    LOG.error("deletePastOldReportAccount failed", e);
                }
                try {
                    Thread.sleep(1000 * 60);
                } catch (InterruptedException ignored) {
                }
            }
        }
        LOG.info("deletePastOldReportAccount end, count:{}, cost:{}", updateCount, System.currentTimeMillis() - start);
    }

}
