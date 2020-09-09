// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.adc.service;

import com.adtiming.om.adc.dto.ReportTask;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
            List<ReportTask> list = jdbcTemplateW.query("SELECT * FROM report_adnetwork_task WHERE status=3 AND run_count>10", ReportTask.ROWMAPPER);
            if (!list.isEmpty()) {
                Set<Integer> taskIds = new HashSet<>();
                Set<Integer> accountIds = new HashSet<>();
                List<Object[]> updateParams = new ArrayList<>();
                for (ReportTask task : list) {
                    taskIds.add(task.id);
                    accountIds.add(task.reportAccountId);
                    updateParams.add(new Object[] {task.msg, task.id});
                }
                count = accountIds.size();
                jdbcTemplateW.update(String.format("update report_adnetwork_task set status=2 where id in (%s)", StringUtils.join(taskIds, ",")));
                LOG.error("Account paused, accountIds:{}", accountIds);
                jdbcTemplateW.batchUpdate("update report_adnetwork_account set status=2,reason=? where id=?", updateParams);
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
                //删除7天前的变更配置
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
