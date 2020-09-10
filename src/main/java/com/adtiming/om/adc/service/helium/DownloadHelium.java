// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3
package com.adtiming.om.adc.service.helium;

import com.adtiming.om.adc.dto.ReportAdnData;
import com.adtiming.om.adc.dto.ReportTask;
import com.adtiming.om.adc.service.AdnBaseService;
import com.adtiming.om.adc.service.AppConfig;
import com.adtiming.om.adc.util.MapHelper;
import com.adtiming.om.adc.util.MyHttpClient;
import com.adtiming.om.adc.util.Util;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class DownloadHelium extends AdnBaseService {

    private static final Logger LOG = LogManager.getLogger();

    @Resource
    private JdbcTemplate jdbcTemplateW;

    @Resource
    private AppConfig cfg;

    @Override
    public void setAdnInfo() {
        this.adnId = 17;
        this.adnName = "Helium";
        this.maxTaskCount = 10;//Reporting API is limited to 40 requests every 10 minutes.
    }

    @Override
    public void executeTaskImpl(ReportTask o) {
        try {
            download(o);
        } catch (Exception e) {
            LOG.error("[{}] executeTaskImpl error, taskId:{}", this.adnName, o.id, e);
        }
    }

    public void download(ReportTask task) {
        String userId = task.userId;
        String userSignature = task.userSignature;
        if (StringUtils.isBlank(userId)) {
            LOG.error("[Helium] userId is null");
            return;
        }
        if (StringUtils.isBlank(userSignature)) {
            LOG.error("[Helium] userSignature is null");
            return;
        }

        LOG.info("[Helium] download start, taskId:{}", task.id);
        long start = System.currentTimeMillis();
        updateTaskStatus(jdbcTemplateW, task.id, 1, "");
        StringBuilder err = new StringBuilder();
        String error;
        task.step = 1;
        String jsonData = downloadData(task, err);
        if (StringUtils.isNotBlank(jsonData) && err.length() == 0) {
            task.step = 2;
            error = jsonDataImportDatabase(task, jsonData);
            if (StringUtils.isBlank(error)) {
                task.step = 3;
                error = savePrepareReportData(task, task.day, userId);
                if (StringUtils.isBlank(error)) {
                    task.step = 4;
                    error = reportLinkedToStat(task, userId);
                }
            }
        } else {
            error = err.toString();
        }
        int status = getStatus(error);
        error = convertMsg(error);
        if (task.runCount >= 4 && status != 2) {
            updateAccountException(jdbcTemplateW, task, error);
            LOG.error("[Helium] executeTaskImpl error,run count:{},taskId:{},msg:{}", task.runCount + 1, task.id, error);
        } else {
            updateTaskStatus(jdbcTemplateW, task.id, status, error);
        }
        LOG.info("[Helium] download finished, taskId:{},cost:{}", task.id, System.currentTimeMillis() - start);
    }

    private String downloadData(ReportTask task, StringBuilder err) {
        String jsonData = "";
        HttpEntity entity = null;
        LOG.info("[Helium] downloadData start, taskId:{}", task.id);
        long start = System.currentTimeMillis();
        try {
            JSONObject param = new JSONObject();
            JSONObject filter = new JSONObject();
            filter.put("network_type", "bidding");
            param.put("filters", filter);
            param.put("date_min", task.day);
            param.put("date_max", task.day);
            param.put("timezone", "UTC");
            String[] dimensions = new String[]{"date", "app", "helium_placement_name", "country", "placement_type", "demand_source"};
            param.put("dimensions", dimensions);
            String[] metrics = new String[]{"requests", "impressions", "estimated_earnings", "valid_bids", "winning_bids"};
            param.put("metrics", metrics);
            String paramsStr = JSON.toJSONString(param);
            String url = String.format("https://helium-api.chartboost.com/v1/publisher/metrics?%s", paramsStr);
            updateReqUrl(jdbcTemplateW, task.id, url);
            HttpPost httpPost = new HttpPost("https://helium-api.chartboost.com/v1/publisher/metrics");
            StringEntity se = new StringEntity(paramsStr, StandardCharsets.UTF_8);
            RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(5 * 60 * 1000).setProxy(cfg.httpProxy).build();
            httpPost.setConfig(requestConfig);
            httpPost.setHeader("Content-Type", "application/json");
            httpPost.setHeader("Cookie", String.format("user_id=%s;user_signature=%s;", task.userId, task.userSignature));
            httpPost.setEntity(se);
            HttpResponse resp = MyHttpClient.getInstance().execute(httpPost);
            StatusLine sl = resp.getStatusLine();
            entity = resp.getEntity();
            if (sl.getStatusCode() != 200) {
                err.append(String.format("request report response statusCode:%d,msg:%s", sl.getStatusCode(), entity == null ? "" : EntityUtils.toString(entity)));
                return jsonData;
            }
            if (entity == null) {
                err.append("request report response enity is null");
                return jsonData;
            }
            jsonData = EntityUtils.toString(entity);
            JSONObject obj = JSONObject.parseObject(jsonData);
            String data = obj.getString("data");
            if (StringUtils.isNoneBlank(data)) {
                return data;
            }
            err.append(jsonData);
            return "";
        } catch (Exception e) {
            err.append(String.format("downJsonData error, msg:%s", e.getMessage()));
        } finally {
            EntityUtils.consumeQuietly(entity);
        }
        LOG.info("[Helium] downloadData end, taskId:{}, cost:{}", task.id, System.currentTimeMillis() - start);
        return jsonData;
    }

    private String jsonDataImportDatabase(ReportTask task, String jsonData) {
        String sql_delete = "delete from report_helium where day=? and user_id=? ";
        try {
            jdbcTemplateW.update(sql_delete, task.day, task.userId);
        } catch (Exception e) {
            return String.format("delete report_mint error,msg:%s", e.getMessage());
        }
        LOG.info("[Helium] jsonDataImportDatabase start, taskId:{}", task.id);
        long start = System.currentTimeMillis();
        String error = "";

        try {
            String sql_insert = "INSERT into report_helium (day, country, app, demand_source, helium_placement_name, placement_type,requests, valid_bids, winning_bids, impressions, estimated_earnings, user_id) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)";

            List<Object[]> lsParm = new ArrayList<>(1000);
            JSONArray jsonArray = JSONArray.parseArray(jsonData);
            for (int i = 0; i < jsonArray.size(); i++) {
                JSONObject obj = jsonArray.getJSONObject(i);
                Object[] params = new Object[]{task.day, obj.get("country"), obj.get("app"),
                        obj.get("demand_source"), obj.get("helium_placement_name"), obj.get("placement_type"),
                        Util.getJSONInt(obj, "requests"),
                        Util.getJSONInt(obj, "valid_bids"),
                        Util.getJSONInt(obj, "winning_bids"),
                        Util.getJSONInt(obj, "impressions"),
                        Util.getJSONDecimal(obj, "estimated_earnings"), task.userId};
                lsParm.add(params);
                if (lsParm.size() == 1000) {
                    jdbcTemplateW.batchUpdate(sql_insert, lsParm);
                    lsParm.clear();
                }
            }
            if (!lsParm.isEmpty()) {
                jdbcTemplateW.batchUpdate(sql_insert, lsParm);
            }
        } catch (Exception e) {
            error = String.format("insert report_mint error, msg:%s", e.getMessage());
        }
        LOG.info("[Helium] jsonDataImportDatabase end, taskId:{}, cost:{}", task.id, System.currentTimeMillis() - start);
        return error;
    }

    private String savePrepareReportData(ReportTask task, String reportDay, String userId) {
        String error;
        LOG.info("[Helium] savePrepareReportData start, taskId:{}", task.id);
        long start = System.currentTimeMillis();
        try {
            List<Map<String, Object>> instanceInfoList = getInstanceList(task.reportAccountId);
            Map<String, Map<String, Object>> placements = instanceInfoList.stream().collect(Collectors.toMap(m -> MapHelper.getString(m, "placement_key"), m -> m, (existingValue, newValue) -> existingValue));

            // instance's placement_key changed
            Set<Integer> insIds = instanceInfoList.stream().map(o->MapHelper.getInt(o, "instance_id")).collect(Collectors.toSet());
            List<Map<String, Object>> oldInstanceList = getOldInstanceList(insIds);
            if (!oldInstanceList.isEmpty()) {
                placements.putAll(oldInstanceList.stream()
                        .filter(m -> !placements.containsKey(MapHelper.getString(m, "placement_key")))
                        .collect(Collectors.toMap(m ->
                        MapHelper.getString(m, "placement_key"), m -> m, (existingValue, newValue) -> existingValue)));
            }

            String dataSql = "select day,country,helium_placement_name data_key,sum(requests) AS api_request,sum(winning_bids) api_filled," +
                    "sum(impressions) AS api_impr,0 AS api_click,sum(estimated_earnings) AS revenue" +
                    " from report_helium where day=? and user_id=? group by day,country,helium_placement_name ";

            List<ReportAdnData> oriDataList = jdbcTemplateW.query(dataSql, ReportAdnData.ROWMAPPER, reportDay, userId);

            if (oriDataList.isEmpty())
                return "data is null";
            error = toAdnetworkLinked(task, userId, placements, oriDataList);
        } catch (Exception e) {
            error = String.format("savePrepareReportData error:%s", e.getMessage());
        }
        LOG.info("[Helium] savePrepareReportData End, taskId:{}, cost:{}", task.id, System.currentTimeMillis() - start);
        return error;
    }
}
