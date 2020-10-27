// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.adc.service.facebook;

import com.adtiming.om.adc.dto.ReportAdnData;
import com.adtiming.om.adc.dto.ReportTask;
import com.adtiming.om.adc.service.AdnBaseService;
import com.adtiming.om.adc.service.AppConfig;
import com.adtiming.om.adc.util.DateTimeFormat;
import com.adtiming.om.adc.util.Http;
import com.adtiming.om.adc.util.MapHelper;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.URLDecoder;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;

@Service
public class DownloadFacebook extends AdnBaseService {

    private static final Logger LOG = LogManager.getLogger();

    private static final String url = "https://graph.facebook.com/v2.10/%s/adnetworkanalytics_results?" +
            "query_ids=['%s']&access_token=%s";

    //private static final String columns = "['fb_ad_network_revenue','fb_ad_network_request','fb_ad_network_cpm','fb_ad_network_click','fb_ad_network_imp','fb_ad_network_filled_request','fb_ad_network_fill_rate','fb_ad_network_ctr','fb_ad_network_show_rate','fb_ad_network_video_guarantee_revenue','fb_ad_network_video_view','fb_ad_network_video_view_rate','fb_ad_network_video_mrc','fb_ad_network_video_mrc_rate','fb_ad_network_bidding_request','fb_ad_network_bidding_response']";
    private static final String columns = "['fb_ad_network_revenue','fb_ad_network_cpm','fb_ad_network_request','fb_ad_network_click','fb_ad_network_imp','fb_ad_network_filled_request','fb_ad_network_video_guarantee_revenue','fb_ad_network_video_view','fb_ad_network_video_mrc','fb_ad_network_bidding_request','fb_ad_network_bidding_response']";
    private static final String breakdowns = "['country','app','placement','platform']";
    private static final String getQueryIdUrl = "https://graph.facebook.com/v2.11/%s/adnetworkanalytics/?since=%s&until=%s&aggregation_period=day&metrics=%s&breakdowns=%s&access_token=%s";

    @Resource
    private AppConfig cfg;

    @Resource
    private JdbcTemplate jdbcTemplate;

    @Override
    public void setAdnInfo() {
        this.adnId = 3;
        this.adnName = "facebook";
        this.maxTaskCount = 50;//FB You can have at most 50 queries per minute
    }

    @Override
    public void executeTaskImpl(ReportTask o) {
        try {
            downloadFacebookData(o);
        } catch (Exception e) {
            LOG.error("[{}] executeTaskImpl error, taskId:{}", this.adnName, o.id);
        }
    }

    private void downloadFacebookData(ReportTask task) {
        LOG.info("[facebook] download report start，task_id:{}", task.id);
        long start = System.currentTimeMillis();
        try {
            //改任务状态
            updateTaskStatus(jdbcTemplate, task.id, 1, "start");//start
            task.step = 1;
            JSONArray results = requestFBApi(task);
            if (results == null) {
                LOG.warn("[facebook] download report response [data->results] is null, task:{}", JSONObject.toJSONString(task));
                //状态更新
                updateTaskStatus(jdbcTemplate, task.id, 3, "report response [data->results] is null");
                return;
            }
            if (results.isEmpty()) {
                //状态更新
                updateTaskStatus(jdbcTemplate, task.id, 2, "results is empty");
                LOG.info("[facebook] download report end, has no data to update, queryId:{},cost:{}", task.queryId, System.currentTimeMillis() - start);
                return;
            }
            Map<String, Object[]> insertMap = new HashMap<>();
            for (int i = 0; i < results.size(); i++) {
                JSONObject res = results.getJSONObject(i);
                JSONArray breakdowns = res.getJSONArray("breakdowns");
                if (breakdowns == null) {
                    continue;
                }
                String country = "";
                String app = "";
                String placement = "";
                String platform = "";
                for (int j = 0; j < breakdowns.size(); j++) {
                    JSONObject breakdown = breakdowns.getJSONObject(j);
                    if (breakdown == null) {
                        continue;
                    }
                    String key = breakdown.getString("key");
                    switch (key) {
                        case "country":
                            country = StringUtils.defaultString(breakdown.getString("value"));
                            break;
                        case "app":
                            app = StringUtils.defaultString(breakdown.getString("value"));
                            break;
                        case "placement":
                            placement = StringUtils.defaultString(breakdown.getString("value"));
                            break;
                        default:
                            platform = StringUtils.defaultString(breakdown.getString("value"));
                            break;
                    }
                }
                LocalDateTime time = LocalDateTime.parse(res.getString("time"), DateTimeFormat.ISO_FORMAT);//2020-01-06T08:00:00+0000
                String day = DateTimeFormat.DAY_FORMAT.format(time);
                String hour = DateTimeFormat.HOUR_FORMAT.format(time);
                String key = String.format("%s_%s_%s_%s_%s_%s", day, hour, country, platform, app, placement);
                Object[] insertObj;
                if (insertMap.containsKey(key)) {
                    insertObj = insertMap.get(key);
                } else {
                    insertObj = new Object[]{day, hour, country, platform, app, placement,
                            BigDecimal.ZERO, 0L, BigDecimal.ZERO, 0L, 0L, 0L, BigDecimal.ZERO, BigDecimal.ZERO,
                            BigDecimal.ZERO, BigDecimal.ZERO, 0L, BigDecimal.ZERO, 0L, BigDecimal.ZERO, 0L, 0L};
                }
                String metric = res.getString("metric");
                String value = res.getString("value");
                setDataRow(insertObj, metric, value);
                insertMap.put(key, insertObj);
            }
            task.step = 2;
            String error = saveData(insertMap, task);
            if (StringUtils.isBlank(error)) {
                task.step = 3;
                error = savePrepareReportData(task, task.day, task.adnAppId);
                if (StringUtils.isBlank(error)) {
                    task.step = 4;
                    error = reportLinkedToStat(task, task.adnAppId);
                }
            }
            int status = getStatus(error);
            error = convertMsg(error);
            if (task.runCount >= 4 && status != 2) {
                updateAccountException(jdbcTemplate, task, error);
                LOG.error("[facebook] executeTaskImpl error,run count:{},taskId:{},msg:{}", task.runCount + 1, task.id, error);
            } else {
                updateTaskStatus(jdbcTemplate, task.id, status, error);
            }

        } catch (Exception e) {
            updateTaskStatus(jdbcTemplate, task.id, 3, e.getMessage());
        }
        LOG.info("[facebook] download report end，task_id:{},cost:{}", task.id, System.currentTimeMillis() - start);
    }

    private JSONArray requestFBApi(ReportTask task) throws IOException {
        String reqUrl = task.reqUrl;
        if (StringUtils.isNoneBlank(reqUrl)) {
            reqUrl = URLDecoder.decode(reqUrl, "UTF-8");
        } else {
            reqUrl = String.format(url, task.adnAppId, task.queryId, task.adnAppToken);
        }
        JSONArray results = getResultsData(reqUrl);
        if (results == null) {
            String go = String.format(getQueryIdUrl, task.adnAppId, task.day, task.day, columns, breakdowns, task.adnAppToken);
            String queryIdResp = Http.post(go, 60000, cfg.httpProxy);
            if (StringUtils.isNoneBlank(queryIdResp)) {
                JSONObject json = JSON.parseObject(queryIdResp);
                String queryId = json.getString("queryId");
                String asyncResultLink = json.getString("async_result_link");
                if (StringUtils.isNoneBlank(queryId) && StringUtils.isNoneBlank(asyncResultLink)) {
                    int reqCount = 1;
                    String reqRetUrl = URLDecoder.decode(asyncResultLink, "UTF-8");
                    while (reqCount < 5) {
                        results = getResultsData(reqRetUrl);
                        if (results != null) {
                            break;
                        }
                        reqCount++;
                        try {
                            Thread.sleep(1000 * 60);//间隔10秒获取一次,等待60秒
                        } catch (InterruptedException ignored) {
                        }
                    }
                }

            }
        }
        return results;
    }

    private JSONArray getResultsData(String reqUrl) throws IOException {
        if (StringUtils.isNoneBlank(reqUrl)) {
            reqUrl = URLDecoder.decode(reqUrl, "UTF-8");
        }
        String resp = Http.get(reqUrl, 120000, cfg.httpProxy);
        if (StringUtils.isNoneBlank(resp)) {
            JSONObject obj = JSON.parseObject(resp);
            JSONArray data = obj.getJSONArray("data");
            if (data != null && !data.isEmpty()) {
                JSONObject dataObj = data.getJSONObject(0);
                String status = dataObj.getString("status");
                if (status.equalsIgnoreCase("complete")) {
                    return dataObj.getJSONArray("results");
                }
            }
        }
        return null;
    }

    private void setDataRow(Object[] row, String metric, String columnValue) {
        if (StringUtils.isBlank(columnValue))
            return;
        switch (metric) {
            case "fb_ad_network_revenue":
                row[6] = turnBigDecimal(columnValue).add((BigDecimal) row[6]);
                break;
            case "fb_ad_network_request":
                row[7] = NumberUtils.toLong(columnValue) + (long) row[7];
                break;
            case "fb_ad_network_cpm":
                row[8] = turnBigDecimal(columnValue).add((BigDecimal) row[8]);
                break;
            case "fb_ad_network_click":
                row[9] = NumberUtils.toLong(columnValue) + (long) row[9];
                break;
            case "fb_ad_network_imp":
                row[10] = NumberUtils.toLong(columnValue) + (long) row[10];
                break;
            case "fb_ad_network_filled_request":
                row[11] = NumberUtils.toLong(columnValue) + (long) row[11];
                break;
            case "fb_ad_network_fill_rate":
                row[12] = turnBigDecimal(columnValue).add((BigDecimal) row[12]);
                break;
            case "fb_ad_network_ctr":
                row[13] = turnBigDecimal(columnValue).add((BigDecimal) row[13]);
                break;
            case "fb_ad_network_show_rate":
                row[14] = turnBigDecimal(columnValue).add((BigDecimal) row[14]);
                break;
            case "fb_ad_network_video_guarantee_revenue":
                row[15] = turnBigDecimal(columnValue).add((BigDecimal) row[15]);
                break;
            case "fb_ad_network_video_view":
                row[16] = NumberUtils.toLong(columnValue) + (long) row[16];
                break;
            case "fb_ad_network_video_view_rate":
                row[17] = turnBigDecimal(columnValue).add((BigDecimal) row[17]);
                break;
            case "fb_ad_network_video_mrc":
                row[18] = NumberUtils.toLong(columnValue) + (long) row[18];
                break;
            case "fb_ad_network_video_mrc_rate":
                row[19] = turnBigDecimal(columnValue).add((BigDecimal) row[19]);
                break;
            case "fb_ad_network_bidding_request":
                row[20] = NumberUtils.toLong(columnValue) + (long) row[20];
                break;
            case "fb_ad_network_bidding_response":
                row[21] = NumberUtils.toLong(columnValue) + (long) row[21];
                break;
        }
    }

    private BigDecimal turnBigDecimal(String value) {
        BigDecimal bg;
        try {
            bg = new BigDecimal(value);
        } catch (Exception e) {
            bg = BigDecimal.ZERO;
        }
        return bg;
    }

    private String saveData(Map<String, Object[]> data, ReportTask task) {
        if (task == null) {
            return "task is null";
        }
        String appId = task.adnAppId;
        if (data.isEmpty()) {
            return "data is null";
        }

        Set<String> dataDays = new HashSet<>();
        for (Object[] value : data.values()) {
            String v = value[0].toString();
            dataDays.add(v);
        }
        String deleteSql = String.format("delete from report_facebook where day in ('%s') and app='%s'", StringUtils.join(dataDays, "','"), appId);
        try {
            jdbcTemplate.update(deleteSql);
        } catch (Exception e) {
            return String.format("delete report_facebook error,msg:%s", e.getMessage());
        }
        String error = "";
        LOG.info("[facebook] insert report_facebook start,app_id:" + appId);
        int count = 0;
        long start = System.currentTimeMillis();
        try {
            String insertSql = "INSERT into report_facebook (day,hour,country,platform,app,placement,revenue," +
                    "request,cpm,click,imp,filled_request,fill_rate,ctr,show_rate,video_guarantee_revenue," +
                    "video_view,video_view_rate,video_mrc,video_mrc_rate,bidding_request,bidding_response)  " +
                    " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
            List<Object[]> lsParm = new ArrayList<>();

            for (Object[] value : data.values()) {
                count++;
                if (lsParm.size() >= 1000) {
                    jdbcTemplate.batchUpdate(insertSql, lsParm);
                    lsParm.clear();
                }
                lsParm.add(value);
            }
            if (!lsParm.isEmpty()) {
                jdbcTemplate.batchUpdate(insertSql, lsParm);
            }
        } catch (Exception e) {
            error = String.format("insert report_facebook error, msg:%s", e.getMessage());
        }
        LOG.info("[facebook] insert report_facebook end,count:{}, cost:{}", count, System.currentTimeMillis() - start);
        return error;
    }

    private String savePrepareReportData(ReportTask task, String reportDay, String appId) {
        long start = System.currentTimeMillis();
        LOG.info("[facebook] savePrepareReportData start, taskId:{}", task.id);
        String error;
        try {
            List<Map<String, Object>> instanceInfoList = getInstanceList(task.reportAccountId);
            /*Map<String, Map<String, Object>> placements = instanceInfoList.stream().collect(Collectors.toMap(m -> MapHelper.getString(m, "placement_key"), m -> m, (existingValue, newValue) -> existingValue));

            // instance's placement_key changed
            Set<Integer> insIds = instanceInfoList.stream().map(o->MapHelper.getInt(o, "instance_id")).collect(Collectors.toSet());
            List<Map<String, Object>> oldInstanceList = getOldInstanceList(insIds);
            if (!oldInstanceList.isEmpty()) {
                placements.putAll(oldInstanceList.stream().collect(Collectors.toMap(m ->
                        MapHelper.getString(m, "placement_key"), m -> m, (existingValue, newValue) -> existingValue)));
            }*/
            if (instanceInfoList.isEmpty()) {
                return "instance is null";
            }
            LocalDate dataDay = LocalDate.parse(task.day, DATEFORMAT_YMD);
            Map<String, Map<String, Object>> placements = new HashMap<>();
            for (Map<String, Object> ins : instanceInfoList) {
                String key = MapHelper.getString(ins, "placement_key");
                putLinkKeyMap(placements, key, ins, dataDay);
            }

            String dataSql = "select day,country,platform,concat(app,'_',placement) data_key," +
                    "sum(revenue) AS revenue,sum(request) AS api_request," +
                    "sum(click) AS api_click,sum(imp) AS api_impr,sum(filled_request) AS api_filled" +
                    " from report_facebook " +
                    " where day =? and app=? " +
                    " group by day,country,platform,app,placement";

            List<ReportAdnData> oriDataList = jdbcTemplate.query(dataSql, ReportAdnData.ROWMAPPER, reportDay, appId);
            if (oriDataList.isEmpty())
                return "data is null";
            error = toAdnetworkLinked(task, appId, placements, oriDataList);
        } catch (Exception e) {
            error = String.format("savePrepareReportData error:%s", e.getMessage());
        }
        LOG.info("[facebook] savePrepareReportData end,taskId:{}, cost:{}", task.id, System.currentTimeMillis() - start);
        return error;
    }
}
