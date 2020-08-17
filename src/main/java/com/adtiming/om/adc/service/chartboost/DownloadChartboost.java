// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.adc.service.chartboost;

import com.adtiming.om.adc.dto.ReportAdnData;
import com.adtiming.om.adc.dto.ReportTask;
import com.adtiming.om.adc.service.AdnBaseService;
import com.adtiming.om.adc.service.AppConfig;
import com.adtiming.om.adc.util.MapHelper;
import com.adtiming.om.adc.util.MyHttpClient;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class DownloadChartboost extends AdnBaseService {

    private static final Logger LOG = LogManager.getLogger();

    @Resource
    private JdbcTemplate jdbcTemplate;

    @Resource
    private AppConfig cfg;

    @Value("${download.dir}")
    private String download_dir;

    @Override
    public void setAdnInfo() {
        this.adnId = 12;
        this.adnName = "Chartboost";
    }

    @Override
    public void executeTaskImpl(ReportTask o) {
        try {
            download(o);
        } catch (Exception e) {
            LOG.error("[{}] executeTaskImpl error, taskId:{}", this.adnName, o.id, e);
        }
    }

    private void download(ReportTask task) {
        String userId = task.userId;
        String userSignature = task.userSignature;
        String date = task.day;
        // Set up AdSense Management API client.
        if (StringUtils.isBlank(userId)) {
            LOG.error("[Chartboost] userId is null");
            return;
        }
        if (StringUtils.isBlank(userSignature)) {
            LOG.error("[Chartboost] userSignature is null");
            return;
        }

        Calendar cal = Calendar.getInstance();
        LOG.info("[Chartboost] download start, userId:{},date:{}, timeZone:{}", userId, date, cal.getTimeZone().toZoneId().getId());
        long start = System.currentTimeMillis();
        updateTaskStatus(jdbcTemplate, task.id, 1, "");
        List<Map<String, Object>> instanceInfoList = getInstanceList(task.reportAccountId);

        Set<Integer> insIds = instanceInfoList.stream().map(o->MapHelper.getInt(o, "instance_id")).collect(Collectors.toSet());
        List<Map<String, Object>> oldInstance = getOldInstanceList(insIds);
        if (!oldInstance.isEmpty()) {
            instanceInfoList.addAll(oldInstance);
        }
        String error;
        //List<String> data = jdbcTemplate.queryForList(instanceSql, String.class, userId);
        task.step = 1;
        if (!instanceInfoList.isEmpty()) {
            List<Boolean> bl = new ArrayList<>();
            for (Map<String, Object> ins : instanceInfoList) {
                String placementKey = MapHelper.getString(ins, "placement_key");
                if (StringUtils.isBlank(placementKey))
                    continue;
                StringBuilder sb = new StringBuilder();
                task.step = 1;
                String jsonData = downJsonData(userId, userSignature, date, date, placementKey, sb);
                if (StringUtils.isNoneBlank(jsonData) && sb.length() == 0) {//request error
                    task.step = 2;
                    String err = jsonDataImportDatabase(jsonData, date, userId, placementKey);
                    boolean flag = StringUtils.isBlank(err) || "data is null".equals(err);
                    bl.add(flag);
                }
            }
            if (bl.size() == instanceInfoList.size()) {
                task.step = 3;
                error = savePrepareReportData(task, date, userId);
                if (StringUtils.isBlank(error)) {
                    task.step = 4;
                    error = reportLinkedToStat(task, userId);
                }
            } else {
                error = "downJsonData failed";
            }
        } else {
            error = "instance is null";
        }
        int status = getStatus(error);
        error = convertMsg(error);
        if (task.runCount > 5 && task.runCount % 5 == 0 && status != 2) {
            updateAccountException(jdbcTemplate, task, error);
            LOG.error("[Chartboost] executeTaskImpl error,run count:{},taskId:{},msg:{}", task.runCount + 1, task.id, error);
        } else {
            updateTaskStatus(jdbcTemplate, task.id, status, error);
        }
        LOG.info("[Chartboost] download finished, userId:{},date:{},cost:{}", userId, date, System.currentTimeMillis() - start);
    }

    private String downJsonData(String userId, String userSignature, String startDate, String endDate, String adLocation, StringBuilder err) {
        String jsonData = "";
        HttpEntity entity = null;
        int count = 1;
        String url;
        try {
            url = "https://analytics.chartboost.com/v3/metrics/appcountry?" + "dateMin=" + startDate + "&dateMax=" + endDate +
                    "&userId=" + userId + "&userSignature=" + userSignature + "&adLocation=" + URLEncoder.encode(adLocation, "utf-8");
        } catch (UnsupportedEncodingException e) {
            err.append("build url error");
            return jsonData;
        }
        LOG.info("[Chartboost] request start, reqUrl:{}", url);
        long start = System.currentTimeMillis();
        while (count < 6) {
            try {
                Thread.sleep(1000L);
            } catch (Exception ignored) {
            }
            try {

                HttpGet httpGet = new HttpGet(url);
                RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(5 * 60 * 1000).setProxy(cfg.httpProxy).build();
                httpGet.setConfig(requestConfig);
                HttpResponse response = MyHttpClient.getInstance().execute(httpGet);
                StatusLine sl = response.getStatusLine();
                entity = response.getEntity();
                if (sl.getStatusCode() != 200) {//如果状态码为200,就是正常返回
                    if (count < 5) {
                        continue;
                    }
                    err.append(String.format("request report response statusCode:%d,msg:%s,url:%s",
                            sl.getStatusCode(), entity == null ? "" : EntityUtils.toString(entity), url));
                    return jsonData;
                }

                if (entity == null) {
                    if (count < 5) {
                        continue;
                    }
                    err.append(String.format("response entity is null,url:%s", url));
                    return jsonData;
                }
                jsonData = EntityUtils.toString(entity);
                break;
            } catch (Exception e) {
                if (count == 5) {
                    err.append(String.format("request error,msg:%s,url:%s", e.getMessage(), url));
                    break;
                }
            } finally {
                EntityUtils.consumeQuietly(entity);
                count++;
            }
        }
        LOG.info("[Chartboost] request end, runCount:{}, reqUrl:{}, cost:{}", count, url, System.currentTimeMillis() - start);
        return jsonData;
    }

    private String jsonDataImportDatabase(String jsonData, String day, String userId, String placementKey) {
        JSONArray jsonArray;
        try {
            jsonArray = JSONArray.parseArray(jsonData);
        } catch (Exception e) {
            return String.format("parse to JSONArray error, jsonData:%s", jsonData);
        }
        if (jsonArray == null || jsonArray.isEmpty()) {
            LOG.warn("[Chartboost] resp data is null, userId:{}, day:{}, placementKey:{}", day, userId, placementKey);
            return "data is null";
        }
        String error = "";
        LOG.info("[Chartboost] jsonDataImportDatabase start, userId:{}, day:{}, placementKey:{}", userId, day, placementKey);
        long start = System.currentTimeMillis();
        String deleteSql = "delete from report_chartboost where day=? and app_key=? and campaign_name=?";
        try {
            jdbcTemplate.update(deleteSql, day, userId, placementKey);
        } catch (Exception e) {
            return String.format("delete report_chartboost error:%s", e.getMessage());
        }
        try {
            String insertSql = "INSERT into report_chartboost (day, country, campaign_name,campaign_type,app_name, " +
                    "   app_id,platform, impressions,clicks," +
                    "  installs, completed_views, ctr, ir, revenue, ecpm, app_key) " +
                    " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

            List<Object[]> lsParm = new ArrayList<>(1000);
            for (int i = 0; i < jsonArray.size(); i++) {
                JSONObject obj = jsonArray.getJSONObject(i);
                Object[] params = new Object[]{day, obj.get("countryCode"), placementKey, obj.get("campaignType"), obj.get("app"),
                        obj.get("appId"), obj.get("platform"), obj.get("impressionsDelivered"), obj.get("clicksDelivered"),
                        obj.get("installsDelivered"), obj.get("videoCompletedDelivered"),
                        obj.get("ctrDelivered") == null ? 0 : obj.get("ctrDelivered"),
                        obj.get("installRateDelivered") == null ? 0 : obj.get("installRateDelivered"),
                        obj.get("moneyEarned") == null ? 0 : obj.get("moneyEarned"), obj.get("ecpmEarned") == null ? 0 : obj.get("ecpmEarned"), userId};
                lsParm.add(params);
                if (lsParm.size() == 1000) {
                    jdbcTemplate.batchUpdate(insertSql, lsParm);
                    lsParm.clear();
                }
            }
            if (!lsParm.isEmpty()) {
                jdbcTemplate.batchUpdate(insertSql, lsParm);
            }
        } catch (Exception e) {
            //log.error("[Chartboost] jsonDataImportDatabase error", e);
            error = String.format("jsonDataImportDatabase error:%s", e.getMessage());
        }
        LOG.info("[Chartboost] jsonDataImportDatabase end, userId:{}, day:{}, placementKey:{}, cost:{}", userId, day, placementKey, System.currentTimeMillis() - start);
        return error;
    }

    private String savePrepareReportData(ReportTask task, String reportDay, String userId) {
        String error = "";
        LOG.info("[Chartboost] savePrepareReportData start, taskId:{}", task.id);
        long start = System.currentTimeMillis();
        try {
            List<Map<String, Object>> instanceInfoList = getInstanceList(task.reportAccountId);
            Map<String, Map<String, Object>> placements = instanceInfoList.stream().collect(Collectors.toMap(m -> MapHelper.getString(m, "placement_key"), m -> m, (existingValue, newValue) -> existingValue));

            // instance's placement_key changed
            Set<Integer> insIds = instanceInfoList.stream().map(o->MapHelper.getInt(o, "instance_id")).collect(Collectors.toSet());
            List<Map<String, Object>> oldInstanceList = getOldInstanceList(insIds);
            if (!oldInstanceList.isEmpty()) {
                placements.putAll(oldInstanceList.stream().collect(Collectors.toMap(m ->
                        MapHelper.getString(m, "placement_key"), m -> m, (existingValue, newValue) -> existingValue)));
            }

            String dataSql = "select day,country,platform,campaign_name data_key," +
                    "sum(impressions) AS api_impr,sum(clicks) AS api_click,sum(revenue) AS revenue" +
                    " from report_chartboost where day=? and app_key=? group by day,country,campaign_name ";

            List<ReportAdnData> oriDataList = jdbcTemplate.query(dataSql, ReportAdnData.ROWMAPPER, reportDay, userId);

            if (oriDataList.isEmpty())
                return "data is null";
            error = toAdnetworkLinked(task, userId, placements, oriDataList);
        } catch (Exception e) {
            error = String.format("savePrepareReportData error:%s", e.getMessage());
        }
        LOG.info("[Chartboost] savePrepareReportData End, taskId:{}, cost:{}", task.id, System.currentTimeMillis() - start);
        return error;
    }
}
