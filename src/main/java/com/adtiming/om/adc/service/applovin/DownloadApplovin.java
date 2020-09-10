// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.adc.service.applovin;

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
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.adtiming.om.adc.util.MapHelper.getInt;

@Service
public class DownloadApplovin extends AdnBaseService {

    private static final Logger LOG = LogManager.getLogger();

    @Resource
    private JdbcTemplate jdbcTemplate;

    @Resource
    private AppConfig cfg;

    @Override
    public void setAdnInfo() {
        this.adnId = 8;
        this.adnName = "Adcolony";
    }

    @Override
    public void executeTaskImpl(ReportTask o) {
        try {
            executeApplovinTask(o);
        } catch (Exception e) {
            LOG.error("[{}] executeTaskImpl error, taskId:{}", this.adnName, o.id, e);
        }
    }

    private void executeApplovinTask(ReportTask task) {
        String appId = task.adnAppId;
        String apiKey = task.adnApiKey;
        String day = task.day;
        // Set up AdSense Management API client.
        if (StringUtils.isBlank(appId)) {
            LOG.error("Applovin，appKey is null");
            return;
        }


        LOG.info("[Applovin] executeTaskImpl start, appId:{}, apiKey:{}, day:{}", appId, apiKey, day);
        long start = System.currentTimeMillis();
        updateTaskStatus(jdbcTemplate, task.id, 1, "");
        StringBuilder err = new StringBuilder();
        String error;
        task.step = 1;
        String json_data = downJsonData(task, appId, apiKey, day, err);
        if (StringUtils.isNotBlank(json_data) && err.length() == 0) {
            task.step = 2;
            error = jsonDataImportDatabase(json_data, day, appId, apiKey);
            if (StringUtils.isBlank(error)) {
                task.step = 3;
                error = savePrepareReportData(task, day, appId);
                if (StringUtils.isBlank(error)) {
                    task.step = 4;
                    error = reportLinkedToStat(task, appId);
                }
            }
        } else {
            error = err.toString();
        }
        int status = getStatus(error);
        error = convertMsg(error);
        if (task.runCount >= 4 && status != 2) {
            updateAccountException(jdbcTemplate, task, error);
            LOG.error("[Applovin] executeTaskImpl error,run count:{},taskId:{},msg:{}", task.runCount + 1, task.id, error);
        } else {
            updateTaskStatus(jdbcTemplate, task.id, status, error);
        }
        LOG.info("[Applovin] executeTaskImpl end, appId:{}, apiKey:{}, day:{}, cost:{}", appId, apiKey, day, System.currentTimeMillis() - start);
    }

    private String downJsonData(ReportTask task, String appId, String apiKey, String day, StringBuilder err) {
        int taskId = task.id;
        String url = String.format("https://r.applovin.com/report?api_key=%s&columns=%s,impressions,clicks,ctr,revenue,ecpm,country,ad_type,size,device_type,platform,application,package_name,placement,application_is_hidden,zone,zone_id&format=json&start=%s&end=%s",
                apiKey, task.timeDimension == 0 ? "day,hour" : "day", day, day);
        String jsonData = "";
        HttpEntity entity = null;
        LOG.info("[Applovin] downJsonData start, taskId:{}, appId:{}, apiKey:{}, day:{}", taskId, appId, apiKey, day);
        LOG.info("[Applovin] request url:{}", url);
        long start = System.currentTimeMillis();
        try {
            updateReqUrl(jdbcTemplate, taskId, url);
            LOG.info("Applovin:" + LocalDateTime.now().toLocalTime());
            HttpGet httpGet = new HttpGet(url);
            RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(5 * 60 * 1000).setProxy(cfg.httpProxy).build();
            httpGet.setConfig(requestConfig);
            HttpResponse response = MyHttpClient.getInstance().execute(httpGet);
            StatusLine sl = response.getStatusLine();
            entity = response.getEntity();
            if (sl.getStatusCode() != 200) {
                err.append(String.format("request report response statusCode:%d,msg:%s", sl.getStatusCode(), entity == null ? "" : EntityUtils.toString(entity)));
                return jsonData;
            }
            if (entity == null) {
                err.append("request report response enity is null");
                return jsonData;
            }
            jsonData = EntityUtils.toString(entity);
        } catch (Exception ex) {
            err.append(String.format("downJsonData error,msg:%s", ex.getMessage()));
        } finally {
            EntityUtils.consumeQuietly(entity);
        }
        LOG.info("[Applovin] downJsonData end, taskId:{}, appId:{}, apiKey:{}, day:{}, cost:{}", taskId, appId, apiKey, day, System.currentTimeMillis() - start);
        return jsonData;
    }

    private String jsonDataImportDatabase(String jsonData, String day, String appId, String apiKey) {
        try {
            String deleteSql = "DELETE FROM report_applovin WHERE day=? AND sdkKey=?";
            jdbcTemplate.update(deleteSql, day, appId);
        } catch (Exception e) {
            return String.format("delete report_applovin error,msg:%s", e.getMessage());
        }
        LOG.info("[Applovin] jsonDataImportDatabase start, appId:{}, apiKey:{}, day:{}", appId, apiKey, day);
        long start = System.currentTimeMillis();
        String error = "";
        String insertSql = "INSERT INTO report_applovin (day,hour,country,platform,application,package_name,placement," +
                "ad_type,device_type,application_is_hidden,zone,zone_id,size,impressions,clicks,ctr,revenue,ecpm,sdkKey)  " +
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        try {
            int count = 0;
            List<Object[]> lsParm = new ArrayList<>();
            JSONObject jobj = JSONObject.parseObject(jsonData);
            if (StringUtils.isBlank(jobj.getString("results")))
                return "response results is null";

            JSONArray jsonArray = JSONArray.parseArray(jobj.getString("results"));
            for (int i = 0; i < jsonArray.size(); i++) {
                JSONObject obj = jsonArray.getJSONObject(i);
                String hour = MapHelper.getString(obj, "hour");
                if (StringUtils.isBlank(hour)) {
                    hour = "00:00";
                }
                count++;
                Object[] params = new Object[]{obj.get("day"), hour, obj.get("country"), obj.get("platform"), obj.get("application"),
                        obj.get("package_name"), obj.get("placement"), obj.get("ad_type"), obj.get("device_type"), obj.get("application_is_hidden"),
                        obj.get("zone"), obj.get("zone_id"), obj.get("size"), obj.get("impressions"), obj.get("clicks"),
                        obj.get("ctr") == null ? 0 : obj.get("ctr"), obj.get("revenue") == null ? 0 : obj.get("revenue"),
                        obj.get("ecpm") == null ? 0 : obj.get("ecpm"), appId};
                if (count > 1000) {
                    jdbcTemplate.batchUpdate(insertSql, lsParm);
                    count = 1;
                    lsParm = new ArrayList<>();
                }
                lsParm.add(params);
            }
            if (!lsParm.isEmpty()) {
                jdbcTemplate.batchUpdate(insertSql, lsParm);
            }
        } catch (Exception e) {
            error = String.format("insert report_applovin error, msg:%s", e.getMessage());
        }
        LOG.info("[Applovin] jsonDataImportDatabase end, appId:{}, apiKey:{}, day:{}, cost:{}", appId, apiKey, day, System.currentTimeMillis() - start);
        return error;
    }

    private String savePrepareReportData(ReportTask task, String reportDay, String appId) {
        LOG.info("[Applovin] savePrepareReportData start, taskId:{}", task.id);
        long start = System.currentTimeMillis();
        String error;
        try {
            List<Map<String, Object>> instanceInfoList = getInstanceList(task.reportAccountId);

            Map<String, Map<String, Object>> placements = instanceInfoList.stream().collect(Collectors.toMap(m ->
                    MapHelper.getString(m, "placement_key"), m -> m, (existingValue, newValue) -> existingValue));

            // instance's placement_key changed
            Set<Integer> insIds = instanceInfoList.stream().map(o-> getInt(o, "instance_id")).collect(Collectors.toSet());
            List<Map<String, Object>> oldInstanceList = getOldInstanceList(insIds);
            if (!oldInstanceList.isEmpty()) {
                placements.putAll(oldInstanceList.stream().collect(Collectors.toMap(m ->
                        MapHelper.getString(m, "placement_key"), m -> m, (existingValue, newValue) -> existingValue)));
            }

            String dataSql = "select day,left(ifnull(hour,0),2) hour,country,platform,zone_id data_key," +
                    //"case when zone_id is null or zone_id='' then package_name else  zone_id end data_key," +
                    "sum(impressions) AS api_impr,sum(clicks) AS api_click,sum(revenue) AS revenue" +
                    " from report_applovin where day=? and sdkKey=? " +
                    " group by hour,day,country,package_name,zone_id ";

            List<ReportAdnData> oriDataList = jdbcTemplate.query(dataSql, ReportAdnData.ROWMAPPER, reportDay, appId);

            if (oriDataList.isEmpty())
                return "data is empty";

            error = toAdnetworkLinked(task, appId, placements, oriDataList);

        } catch (Exception e) {
            error = String.format("savePrepareReportData error, msg:%s", e.getMessage());
        }
        LOG.info("[Applovin] savePrepareReportData end, taskId:{}, cost:{}", task.id, System.currentTimeMillis() - start);
        return error;
    }
}
