// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.adc.service;

import com.adtiming.om.adc.dto.ReportAdnData;
import com.adtiming.om.adc.dto.ReportTask;
import com.adtiming.om.adc.util.MapHelper;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public abstract class AdnBaseService {

    private static final Logger LOG = LogManager.getLogger();

    @Resource
    private JdbcTemplate jdbcTemplate;

    private BlockingQueue<ReportTask> taskQueue = new LinkedBlockingQueue<>(100);

    @Resource
    private ThreadPoolTaskExecutor executor;

    protected int adnId;
    protected String adnName;

    @PostConstruct
    public void init() {
        setAdnInfo();
        putTaskToQueue(1);
        run();
    }

    private Set<Integer> executeTask = new HashSet<>();

    @Scheduled(cron = "0 * * * * ?")
    public void run() {
        putTaskToQueue(0);
    }

    @Scheduled(cron = "0 */20 * * * ?")
    public void runFailedTask() {
        putTaskToQueue(3);
    }

    private void putTaskToQueue(int status) {
        LOG.info("[{}] put taskQueue Start", adnName);
        long start = System.currentTimeMillis();
        final int[] count = {0};
        try {
            String sql = String.format("select * from report_adnetwork_task where adn_id=%d and status=%d order by id limit 100", adnId, status);
            List<ReportTask> taskList = jdbcTemplate.query(sql, ReportTask.ROWMAPPER);
            if (!taskList.isEmpty()) {
                int maxAllowedSize = 100 - taskQueue.size();
                if (taskList.size() < maxAllowedSize) {
                    maxAllowedSize = taskList.size();
                }
                for (int i = 0; i < maxAllowedSize; i++) {
                    ReportTask task = taskList.get(i);
                    if (!executeTask.contains(task.id)) {
                        taskQueue.put(task);
                        executeTask.add(task.id);
                        count[0]++;
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("[{}] put taskQueue error", adnName, e);
        }
        LOG.info("[{}] put taskQueue, count:{}, cost:{}", adnName, count[0], System.currentTimeMillis() - start);
    }


    @Scheduled(fixedDelay = 1000 * 60)
    public void runTask() {
        try {
            if (taskQueue.isEmpty()) return;
            LOG.info("[{}] runTask start,count:{}", adnName, taskQueue.size());
            long start = System.currentTimeMillis();
            int count = taskQueue.size();
            //FB 一分钟内只允许50个查询，You can have at most 50 queries per minute
            if (adnId == 2 && count > 50) {
                count = 50;
            }
            CountDownLatch cd = new CountDownLatch(count);
            for (int i = 0; i < count; i++) {
                ReportTask o = taskQueue.poll();
                executor.execute(() -> {
                    executeTask(o);
                    cd.countDown();
                });
            }
            //60分钟超时时间
            cd.await(60, TimeUnit.MINUTES);
            LOG.info("[{}] runTask finished,count:{}, cost:{}", adnName, count, System.currentTimeMillis() - start);
        } catch (Exception e) {
            LOG.error("[{}] runTask error", adnName, e);
        }
    }

    private void executeTask(ReportTask o) {
        executeTaskImpl(o);
        finishCallBack(o);
    }

    protected void updateTaskStatus(JdbcTemplate jdbcTemplateW, int id, int status, String msg) {
        String sql;
        if (status == 1) {
            sql = "UPDATE report_adnetwork_task set status=? WHERE id=?";
            jdbcTemplateW.update(sql, status, id);
        } else {
            if (StringUtils.isBlank(msg)) {
                sql = "UPDATE report_adnetwork_task SET status=?,run_count=run_count+1 WHERE id=?";
                jdbcTemplateW.update(sql, status, id);
            } else {
                sql = String.format("UPDATE report_adnetwork_task SET status=?,msg=concat(ifnull(msg,''), '\n', '%s'),run_count=run_count+1 WHERE id=?", msg.replaceAll("'", "''"));
                jdbcTemplateW.update(sql, status, id);
            }
        }
    }

    protected void updateReqUrl(JdbcTemplate jdbcTemplate, int id, String reqUrl) {
        if (StringUtils.isNotBlank(reqUrl)) {
            String sql = "UPDATE report_adnetwork_task SET req_url=? WHERE id=?";
            jdbcTemplate.update(sql, reqUrl, id);
        }
    }

    public abstract void setAdnInfo();

    public abstract void executeTaskImpl(ReportTask o);

    private void finishCallBack(ReportTask o) {
        executeTask.remove(o.id);
    }

    protected List<Map<String, Object>> getInstanceList(String whereSql, String changedSql) {
        String sql = "SELECT a.id AS instance_id,a.placement_key,a.placement_id,a.pub_app_id,a.adn_id,a.ab_test_mode," +
                "b.adn_app_key,c.publisher_id,c.ad_type," +
                "d.plat,d.category,d.app_id," +
                "IFNULL(e.revenue_sharing,1) AS revenue_sharing" +
                " FROM om_instance a" +
                " LEFT JOIN om_adnetwork_app b ON a.adn_app_id=b.id" +
                " LEFT JOIN om_placement c ON a.placement_id=c.id" +
                " LEFT JOIN om_publisher_app d ON a.pub_app_id=d.id" +
                " LEFT JOIN om_publisher e ON c.publisher_id=e.id" +
                " WHERE a.adn_id=? AND " + whereSql;
        List<Map<String, Object>> list = jdbcTemplate.queryForList(sql, adnId);
        List<Map<String, Object>> oldList = getAccountChangedList(changedSql);
        if (!oldList.isEmpty()) {
            list.addAll(oldList);
        }
        return list;
    }
    private List<Map<String, Object>> getAccountChangedList(String whereSql) {
        String sql = "SELECT a.id AS instance_id,a.placement_key,a.placement_id,a.pub_app_id,a.adn_id,a.ab_test_mode," +
                "b.adn_app_key,c.publisher_id,c.ad_type," +
                "d.plat,d.category,d.app_id," +
                "IFNULL(e.revenue_sharing,1) AS revenue_sharing" +
                " FROM om_instance a" +
                " LEFT JOIN om_adnetwork_app_change b ON a.adn_app_id=b.id" +
                " LEFT JOIN om_placement c ON a.placement_id=c.id" +
                " LEFT JOIN om_publisher_app d ON a.pub_app_id=d.id" +
                " LEFT JOIN om_publisher e ON c.publisher_id=e.id" +
                " WHERE a.adn_id=? AND b.status=1 " +
                " AND a.placement_key is not null and a.placement_key !='' AND " + whereSql;
        return jdbcTemplate.queryForList(sql, adnId);
    }

    protected List<Map<String, Object>> getOldInstanceList(Set<Integer> insIds) {
        if (insIds == null || insIds.isEmpty()) {
            return Collections.emptyList();
        }
        String sql = String.format("select a.id as instance_id,a.placement_key,a.placement_id,a.pub_app_id,a.adn_id,a.ab_test_mode," +
                        "b.adn_app_key," +
                        "c.publisher_id,c.ad_type," +
                        "d.plat,d.category,d.app_id," +
                        "IFNULL(e.revenue_sharing,1) as revenue_sharing" +
                        " from om_instance_change a" +
                        " left join om_adnetwork_app b on a.adn_app_id=b.id" +
                        " left join om_placement c on a.placement_id=c.id" +
                        " left join om_publisher_app d on a.pub_app_id=d.id" +
                        " left join om_publisher e on c.publisher_id=e.id" +
                        " where a.adn_id=? AND a.placement_key is not null and a.placement_key !='' " +
                        " and a.id in (%s)",
                StringUtils.join(insIds, ","));
        return jdbcTemplate.queryForList(sql, adnId);
    }

    protected String toAdnetworkLinked(ReportTask task, String adnAccountKey, Map<String, Map<String, Object>> instanceInfoMap, List<ReportAdnData> data) {
        LOG.info("[{}] toAdnetworkLinked start, taskId:{}, day:{}", adnName, task.id, task.day);
        long start = System.currentTimeMillis();
        String error = "";
        try {
            if (instanceInfoMap.isEmpty())
                return "instance is null";

            if (data.isEmpty())
                return "data is null";

            String deleteSql = "DELETE FROM report_adnetwork_linked WHERE adn_id=? AND day=? AND hour=? AND adn_account_key=?";
            jdbcTemplate.update(deleteSql, adnId, task.day, task.hour, adnAccountKey);
            int count = 0;
            List<Object[]> lsParm = new ArrayList<>();
            String insertSql = "INSERT INTO report_adnetwork_linked(day,hour,country,platform,publisher_id,pub_app_id,placement_id,ad_type,adn_id,instance_id,abt,cost,revenue,api_request,api_filled,api_click,api_impr,api_video_start,api_video_complete,adn_account_key,adn_app_key,adn_placement_key)" +
                    " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
            for (ReportAdnData linked : data) {
                try {
                    String dataKey = linked.dataKey;
                    if (StringUtils.isBlank(dataKey))
                        continue;
                    Map<String, Object> instanceConf = instanceInfoMap.get(dataKey);
                    if (instanceConf != null) {
                        count++;
                        BigDecimal sharing = MapHelper.getBigDecimal(instanceConf, "revenue_sharing");
                        sharing = sharing == null ? new BigDecimal(1.0) : sharing;
                        linked.revenue = linked.revenue == null ? BigDecimal.ZERO : linked.revenue;
                        linked.cost = linked.revenue.multiply(sharing);
                        if (StringUtils.isBlank(linked.country)) {
                            continue;
                        }
                        //facebook country Unknown
                        if (linked.country.length() > 2) {
                            linked.country = "";
                        }
                        //day,hour,country,platform,publisher_id,pub_app_id,placement_id,ad_type,adnId,instance_id,abt,cost,revenue,api_request,api_filled,api_click,api_impr,api_video_start,api_video_complete,adn_pub_id,adnAppId,adn_placement_key
                        lsParm.add(new Object[]{linked.day, linked.hour, linked.country,
                                MapHelper.getInt(instanceConf, "plat"),
                                MapHelper.getInt(instanceConf, "publisher_id"),
                                MapHelper.getInt(instanceConf, "pub_app_id"),
                                MapHelper.getInt(instanceConf, "placement_id"),
                                MapHelper.getInt(instanceConf, "ad_type"),
                                adnId,
                                MapHelper.getInt(instanceConf, "instance_id"),
                                MapHelper.getInt(instanceConf, "ab_test_mode"),
                                linked.cost, linked.revenue, linked.apiRequest,
                                linked.apiFilled, linked.apiClick, linked.apiImpr,
                                linked.apiVideoStart, linked.apiVideoComplete,
                                adnAccountKey, MapHelper.getString(instanceConf, "adn_app_key"),
                                MapHelper.getString(instanceConf, "placement_key")
                        });
                        if (lsParm.size() >= 1000) {
                            jdbcTemplate.batchUpdate(insertSql, lsParm);
                            lsParm = new ArrayList<>();
                        }
                    }
                } catch (Exception ex) {
                    return String.format("data link error,msg:%s", ex.getMessage());
                }
            }
            if (!lsParm.isEmpty()) {
                jdbcTemplate.batchUpdate(insertSql, lsParm);
            }
            LOG.info("[{}] toAdnetworkLinked dataCount:{},matchedCount:{}", adnName, data.size(), count);
        } catch (Exception e) {
            error = String.format("toAdnetworkLinked error, msg:%s", e.getMessage());
        }
        LOG.info("[{}] toAdnetworkLinked end, taskId:{}, day:{}, cost:{}",
                adnName, task.id, task.day, System.currentTimeMillis() - start);
        return error;
    }

    protected String reportLinkedToStat(ReportTask task, String accountKey) {
        LOG.info("[{}] reportLinkedToStat start, taskId:{}, day:{}", adnName, task.id, task.day);
        long start = System.currentTimeMillis();
        String error = "";
        try {
            String sql = "DELETE FROM stat_adnetwork WHERE adn_id=? AND day=? and hour=? AND adn_account_key=?";
            jdbcTemplate.update(sql, adnId, task.day, task.hour, accountKey);
            sql = "INSERT INTO stat_adnetwork(day,hour,country,platform,publisher_id,pub_app_id,placement_id,ad_type,adn_id,instance_id,abt,cost,revenue,api_request,api_filled,api_click,api_impr,api_video_start,api_video_complete,adn_account_key)" +
                    "SELECT day,hour,country,platform,publisher_id,pub_app_id,placement_id,ad_type," +
                    "adn_id,instance_id,abt," +
                    "sum(cost) cost," +
                    "sum(revenue) revenue," +
                    "sum(api_request) api_request," +
                    "sum(api_filled) api_filled," +
                    "sum(api_click) api_click,sum(api_impr) api_impr,sum(api_video_start) api_video_start," +
                    "sum(api_video_complete) api_video_complete,adn_account_key" +
                    " FROM report_adnetwork_linked" +
                    " WHERE adn_id=? AND day=? and hour=? AND adn_account_key=?" +
                    " GROUP BY day,hour,country,platform,publisher_id,pub_app_id,placement_id,ad_type,adn_id,instance_id,abt,adn_account_key";
            jdbcTemplate.update(sql, adnId, task.day, task.hour, accountKey);
        } catch (Exception e) {
            error = String.format("reportLinkedToStat error, msg:%s", e.getMessage());
        }
        LOG.info("[{}] reportLinkedToStat end, taskId:{}, day:{}, cost:{}", adnName, task.id, task.day, System.currentTimeMillis() - start);
        return error;
    }
}
