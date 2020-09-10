// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.adc.service;

import com.adtiming.om.adc.dto.ReportAdnData;
import com.adtiming.om.adc.dto.ReportApiError;
import com.adtiming.om.adc.dto.ReportTask;
import com.adtiming.om.adc.util.JdbcHelper;
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

    private BlockingQueue<ReportTask> taskQueue;

    @Resource
    private ThreadPoolTaskExecutor executor;

    protected int adnId;
    protected String adnName;

    protected int maxTaskCount = 100;

    @PostConstruct
    public void init() {
        setAdnInfo();
        taskQueue = new LinkedBlockingQueue<>(maxTaskCount);
        refreshReportAPIError();
        putTaskToQueue(1);
        run();
    }

    private final Set<Integer> executeTask = new HashSet<>();

    private List<ReportApiError> apiErrorList;

    @Scheduled(cron = "0 */5 * * * ?")
    private void refreshReportAPIError() {
        List<ReportApiError> apiErrorConfiglist = new ArrayList<>();
        jdbcTemplate.query("select * from report_adnetwork_error where adn_id=? and status=1", rs -> {
            ReportApiError apiError = ReportApiError.ROWMAPPER.mapRow(rs, rs.getRow());
            apiErrorConfiglist.add(apiError);
        }, this.adnId);
        apiErrorList = apiErrorConfiglist;
    }

    public ReportApiError matchApiError(String error) {
        for (ReportApiError apiError : apiErrorList) {
            for (String err : apiError.errMsg) {
                if (error.contains(err)) {
                    return apiError;
                }
            }
        }
        return null;
    }

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
            String sql = String.format("select * from report_adnetwork_task where adn_id=%d and status=%d order by id limit %d", adnId, status, maxTaskCount);
            List<ReportTask> taskList = jdbcTemplate.query(sql, ReportTask.ROWMAPPER);
            if (!taskList.isEmpty()) {
                int maxAllowedSize = maxTaskCount - taskQueue.size();
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
            if (count > maxTaskCount) {
                count = maxTaskCount;
            }
            if (adnId == 9 || adnId == 15) {//Mopub、IronSource顺序执行
                for (int i = 0; i < count; i++) {
                    ReportTask o = taskQueue.poll();
                    executeTask(o);
                }
            } else {
                CountDownLatch cd = new CountDownLatch(count);
                for (int i = 0; i < count; i++) {
                    ReportTask o = taskQueue.poll();
                    executor.execute(() -> {
                        executeTask(o);
                        cd.countDown();
                    });
                }
                //60 minutes timeout
                cd.await(60, TimeUnit.MINUTES);
            }
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

    /**
     * update account exception
     *
     */
    protected void updateAccountException(JdbcTemplate jdbcTemplateW, ReportTask task, String msg) {
        try {
            String reason;
            int step = task.step;
            if (step == 1) {//API interface request
                reason = msg;
                ReportApiError apiError = matchApiError(msg);
                if (apiError != null) {
                    if (!apiError.isIgnore()) {//Non-ignore error, update account status to Exception
                        jdbcTemplateW.update("UPDATE report_adnetwork_account SET status=2,reason=?,error_id=? WHERE id=?", reason, apiError.id, task.reportAccountId);
                    } else {//An ignore error alert occurs and only the Reason and error_ID are updated
                        jdbcTemplateW.update("UPDATE report_adnetwork_account SET reason=?,error_id=? WHERE id=?", reason, apiError.id, task.reportAccountId);
                        LOG.error("{} ignore error has occurred,report_account_id:{},error:{}", adnName, task.reportAccountId, reason);
                    }
                } else {
                    long errorId = JdbcHelper.insertReturnId(jdbcTemplateW,"insert into report_adnetwork_error(adn_id,content)values(?,?)", adnId, reason);
                    //New error warning
                    jdbcTemplateW.update("UPDATE report_adnetwork_account SET reason=?,error_id=? WHERE id=?", reason, errorId, task.reportAccountId);
                    LOG.error("{} add new error info,report_account_id:{},error:{}", adnName, task.reportAccountId, reason);
                }
            } else {//Internal error
                if (step == 2) {
                    reason = "Raw data inserted into database error.";
                } else if (step == 3) {
                    reason = "Data association error.";
                } else if (step == 4) {
                    reason = "Data aggregation error.";
                } else {
                    reason = "Server error.";
                }
                jdbcTemplateW.update("UPDATE report_adnetwork_account SET reason=? WHERE id=?", reason, task.reportAccountId);
                LOG.error("{} An unexpected error has occurred,report_account_id:{},error:{}", adnName, task.reportAccountId, msg);
            }
            //Exception
            String sql = String.format("UPDATE report_adnetwork_task SET status=?,msg=concat(ifnull(msg,''), '\n', '%s'),run_count=run_count+1 WHERE id=?", msg.replaceAll("'", "''"));
            jdbcTemplateW.update(sql, 2, task.id);
        } catch (Exception e) {
            LOG.error("updateAccountException error", e);
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

    protected List<Map<String, Object>> getInstanceList(int reportAccountId) {
        String whereSql = String.format("b.report_account_id=%d", reportAccountId);
        String sql = "SELECT a.id AS instance_id,a.placement_key,a.placement_id,a.pub_app_id,a.adn_id,a.ab_test_mode," +
                "b.adn_app_key,c.publisher_id,c.ad_type," +
                "d.plat,d.category,d.app_id," +
                "IFNULL(e.revenue_sharing,1) AS revenue_sharing,a.hb_status" +
                " FROM om_instance a" +
                " LEFT JOIN om_adnetwork_app b ON a.adn_app_id=b.id" +
                " LEFT JOIN om_placement c ON a.placement_id=c.id" +
                " LEFT JOIN om_publisher_app d ON a.pub_app_id=d.id" +
                " LEFT JOIN om_publisher e ON c.publisher_id=e.id" +
                " WHERE a.adn_id=? AND " + whereSql;
        List<Map<String, Object>> list = jdbcTemplate.queryForList(sql, adnId);
        List<Map<String, Object>> oldList = getAccountChangedList(whereSql);
        if (!oldList.isEmpty()) {
            list.addAll(oldList);
        }
        return list;
    }
    private List<Map<String, Object>> getAccountChangedList(String whereSql) {
        String sql = "SELECT a.id AS instance_id,a.placement_key,a.placement_id,a.pub_app_id,a.adn_id,a.ab_test_mode," +
                "b.adn_app_key,c.publisher_id,c.ad_type," +
                "d.plat,d.category,d.app_id," +
                "IFNULL(e.revenue_sharing,1) AS revenue_sharing,a.hb_status" +
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
                        "IFNULL(e.revenue_sharing,1) as revenue_sharing,f.hb_status" +
                        " from om_instance_change a" +
                        " left join om_adnetwork_app b on a.adn_app_id=b.id" +
                        " left join om_placement c on a.placement_id=c.id" +
                        " left join om_publisher_app d on a.pub_app_id=d.id" +
                        " left join om_publisher e on c.publisher_id=e.id" +
                        " left join om_instance f on (a.id=f.id)" +
                        " where a.adn_id=? AND a.placement_key is not null and a.placement_key !='' " +
                        " and a.id in (%s)",
                StringUtils.join(insIds, ","));
        return jdbcTemplate.queryForList(sql, adnId);
    }

    private BigDecimal getCurrencyByDay(String currency, String day) {
        try {
            return jdbcTemplate.queryForObject("SELECT exchange_rate FROM om_currency_exchange_day WHERE day=? AND cur_from=?", BigDecimal.class, day, currency);
        } catch (Exception e) {
            try {
                return jdbcTemplate.queryForObject("SELECT exchange_rate FROM om_currency_exchange WHERE cur_from=?", BigDecimal.class, day, currency);
            } catch (Exception ignored) {
            }
        }
        return new BigDecimal(1);
    }

    protected String toAdnetworkLinked(ReportTask task, String adnAccountKey, Map<String, Map<String, Object>> instanceInfoMap, List<ReportAdnData> data) {
        LOG.info("[{}] toAdnetworkLinked start, taskId:{}, day:{}", adnName, task.id, task.day);
        long start = System.currentTimeMillis();
        String error;
        BigDecimal exchangeRate = new BigDecimal(1);
        if (!task.currency.equals("USD")) {
            exchangeRate = getCurrencyByDay(task.currency, task.day);
        }
        try {
            if (instanceInfoMap.isEmpty())
                return "instance is null";

            if (data.isEmpty())
                return "data is null";
            String whereSql = "";
            error = "";
            if (adnId == 3) {
                whereSql = task.timeDimension == 0 ? "and hour=" + task.hour : "";
            }

            String deleteSql = String.format("DELETE FROM report_adnetwork_linked WHERE adn_id=? AND day=? AND adn_account_key=? %s", whereSql);
            jdbcTemplate.update(deleteSql, adnId, task.day, adnAccountKey);
            int count = 0;
            List<Object[]> lsParm = new ArrayList<>();
            String insertSql = "INSERT INTO report_adnetwork_linked(day,hour,country,platform,publisher_id,pub_app_id,placement_id,ad_type,adn_id,instance_id,abt,currency,exchange_rate,cost,cost_ori,revenue,revenue_ori,api_request,api_filled,api_click,api_impr,api_video_start,api_video_complete,adn_account_key,adn_app_key,adn_placement_key,report_account_id,bid)" +
                    " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
            for (ReportAdnData linked : data) {
                try {
                    String dataKey = linked.dataKey;
                    if (StringUtils.isBlank(dataKey))
                        continue;
                    Map<String, Object> instanceConf = instanceInfoMap.get(dataKey);
                    if (instanceConf != null) {
                        count++;
                        BigDecimal sharing = MapHelper.getBigDecimal(instanceConf, "revenue_sharing");
                        sharing = sharing == null ? new BigDecimal("1.0") : sharing;
                        linked.revenue = linked.revenue == null ? BigDecimal.ZERO : linked.revenue;
                        linked.cost = linked.revenue.multiply(sharing);
                        BigDecimal revenue = linked.revenue.multiply(exchangeRate);
                        BigDecimal cost = linked.cost.multiply(exchangeRate);
                        String country = StringUtils.isNoneBlank(linked.country) ? linked.country.toUpperCase() : "";
                        if (StringUtils.isBlank(country)) {
                            continue;
                        }
                        if (country.equals("UK")) {
                            country = "GB";
                        }
                        //facebook country Unknown
                        if (country.length() > 2) {
                            country = "";
                        }

                        lsParm.add(new Object[]{linked.day, linked.hour, country,
                                MapHelper.getInt(instanceConf, "plat"),
                                MapHelper.getInt(instanceConf, "publisher_id"),
                                MapHelper.getInt(instanceConf, "pub_app_id"),
                                MapHelper.getInt(instanceConf, "placement_id"),
                                MapHelper.getInt(instanceConf, "ad_type"),
                                adnId,
                                MapHelper.getInt(instanceConf, "instance_id"),
                                MapHelper.getInt(instanceConf, "ab_test_mode"),
                                task.currency, exchangeRate,
                                cost, linked.cost, revenue, linked.revenue, linked.apiRequest,
                                linked.apiFilled, linked.apiClick, linked.apiImpr,
                                linked.apiVideoStart, linked.apiVideoComplete,
                                adnAccountKey, MapHelper.getString(instanceConf, "adn_app_key"),
                                MapHelper.getString(instanceConf, "placement_key"),
                                task.reportAccountId,
                                MapHelper.getInt(instanceConf, "hb_status")
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
            String whereSql = "";
            if (adnId == 3) {
                whereSql = task.timeDimension == 0 ? "and hour=" + task.hour : "";
            }
            String sql = "DELETE FROM stat_adnetwork WHERE adn_id=? AND day=? AND adn_account_key=? " + whereSql;
            jdbcTemplate.update(sql, adnId, task.day, accountKey);
            sql = String.format("INSERT INTO stat_adnetwork(day,hour,country,platform,publisher_id,pub_app_id,placement_id,ad_type,adn_id,instance_id,abt,currency,exchange_rate,cost,cost_ori,revenue,revenue_ori,api_request,api_filled,api_click,api_impr,api_video_start,api_video_complete,adn_account_key,report_account_id,bid)" +
                    "SELECT day,hour,country,platform,publisher_id,pub_app_id,placement_id,ad_type," +
                    "adn_id,instance_id,abt,currency,exchange_rate," +
                    "sum(cost) cost,sum(cost_ori) cost_ori," +
                    "sum(revenue) revenue,sum(revenue_ori) revenue_ori," +
                    "sum(api_request) api_request," +
                    "sum(api_filled) api_filled," +
                    "sum(api_click) api_click,sum(api_impr) api_impr,sum(api_video_start) api_video_start," +
                    "sum(api_video_complete) api_video_complete,adn_account_key,report_account_id,bid" +
                    " FROM report_adnetwork_linked" +
                    " WHERE adn_id=? AND day=? %s AND adn_account_key=?" +
                    " GROUP BY day,hour,country,platform,publisher_id,pub_app_id,placement_id,ad_type,adn_id,instance_id,abt,currency,exchange_rate,adn_account_key,bid", whereSql);
            jdbcTemplate.update(sql, adnId, task.day, accountKey);
        } catch (Exception e) {
            error = String.format("reportLinkedToStat error, msg:%s", e.getMessage());
        }
        LOG.info("[{}] reportLinkedToStat end, taskId:{}, day:{}, cost:{}", adnName, task.id, task.day, System.currentTimeMillis() - start);
        return error;
    }

    public int getStatus(String msg) {
        int status = 2;
        if (StringUtils.isNoneBlank(msg)) {
            if ("data is null".equals(msg) || "instance is null".equals(msg) || "data is empty".equals(msg)) {
                status = 2;
            } else {
                status = 3;
            }
        }
        return status;
    }

    public String convertMsg(String msg) {
        if (StringUtils.isNoneBlank(msg)) {
            if ("data is null".equals(msg)) {
                msg = "The API interface returns null data.";
            }
            if ("instance is null".equals(msg)) {
                msg = "The available instance is not configured.";
            }
        }
        return msg;
    }
}
