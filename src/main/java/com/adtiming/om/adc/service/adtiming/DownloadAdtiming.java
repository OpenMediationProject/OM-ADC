// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.adc.service.adtiming;

import com.adtiming.om.adc.dto.ReportAdnData;
import com.adtiming.om.adc.dto.ReportTask;
import com.adtiming.om.adc.service.AdnBaseService;
import com.adtiming.om.adc.service.AppConfig;
import com.adtiming.om.adc.service.CountryService;
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
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class DownloadAdtiming extends AdnBaseService {

    private static final Logger LOG = LogManager.getLogger();

    @Resource
    private JdbcTemplate jdbcTemplate;

    @Resource
    private AppConfig cfg;

    @Resource
    private CountryService countryService;

    @Override
    public void setAdnInfo() {
        adnId = 1;
        adnName = "Adtiming";
    }

    @Override
    public void executeTaskImpl(ReportTask o) {
        try {
            executeAdtimingTask(o);
        } catch (Exception e) {
            LOG.error("[{}] executeTaskImpl error, taskId:{}", this.adnName, o.id, e);
        }
    }

    private void executeAdtimingTask(ReportTask task) {
        String token = task.adnAppToken;
        String day = task.day;
        // Set up AdSense Management API client.
        if (StringUtils.isBlank(token)) {
            LOG.error("Adtiming publisher token is null");
            return;
        }

        LOG.info("[Adtiming] executeTaskImpl start, pubToken:{}, day:{}", token, day);
        long start = System.currentTimeMillis();
        updateTaskStatus(jdbcTemplate, task.id, 1, "");
        StringBuilder err = new StringBuilder();
        String error;
        task.step = 1;
        String json_data = downJsonData(task, token, day, err);
        if (StringUtils.isNotBlank(json_data) && err.length() == 0) {
            task.step = 2;
            error = jsonDataImportDatabase(task, json_data, day, token);
            if (StringUtils.isBlank(error)) {
                task.step = 3;
                error = saveReportData(task);
                if (StringUtils.isBlank(error)) {
                    task.step = 4;
                    error = reportLinkedToStat(task, token);
                }
            }
        } else {
            error = err.toString();
        }
        int status = getStatus(error);
        error = convertMsg(error);
        if (task.runCount >= 4 && status != 2) {
            updateAccountException(jdbcTemplate, task, error);
            LOG.error("[Adtiming] executeTaskImpl error,run count:{},taskId:{},msg:{}", task.runCount + 1, task.id, error);
        } else {
            updateTaskStatus(jdbcTemplate, task.id, status, error);
        }
        LOG.info("[Adtiming] executeTaskImpl end, taskId:{}, cost:{}", task.id, System.currentTimeMillis() - start);
    }

    private String downJsonData(ReportTask task, String pubToken, String day, StringBuilder err) {
        String breakdowns = "app,os,placement,country";
        String metrics = "request,filled,impr,click,videoStart,videoFinish,revenue";
        String timeDimension = task.timeDimension == 1 ? "day" : "hour";
        String url = String.format("http://sdk.adtimingapi.com/report/adt/v3?format=json&encoding=UTF-8&token=%s&start=%s&end=%s&breakdowns=%s&metrics=%s&timeDimension=%s", pubToken, day, day, breakdowns, metrics, timeDimension);
        String json_data = "";
        HttpEntity entity = null;
        LOG.info("[Adtiming] request url:{}", url);
        LOG.info("[Adtiming] downJsonData start, taskId:{}", task.id);
        long start = System.currentTimeMillis();
        try {
            updateReqUrl(jdbcTemplate, task.id, url);
            LOG.info("Adtiming:" + LocalDateTime.now().toLocalTime());
            HttpGet httpGet = new HttpGet(url);
            RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(5 * 60 * 1000).setProxy(cfg.httpProxy).build();
            httpGet.setConfig(requestConfig);
            HttpResponse response = MyHttpClient.getInstance().execute(httpGet);
            StatusLine sl = response.getStatusLine();
            entity = response.getEntity();
            if (sl.getStatusCode() != 200) {
                err.append(String.format("request report response statusCode:%d,msg:%s", sl.getStatusCode(), entity == null ? "" : EntityUtils.toString(entity)));
                return json_data;
            }
            if (entity == null) {
                err.append("request report response enity is null");
                return json_data;
            }
            json_data = EntityUtils.toString(entity);
        } catch (Exception ex) {
            err.append(String.format("downJsonData error,msg:%s", ex.getMessage()));
        } finally {
            EntityUtils.consumeQuietly(entity);
        }
        LOG.info("[Adtiming] downJsonData end, taskId:{}, cost:{}", task.id, System.currentTimeMillis() - start);
        return json_data;
    }

    private String jsonDataImportDatabase(ReportTask task, String jsonData, String day, String pubToken) {
        String deleteSql = String.format("delete from report_adtiming where day=? and pub_token=? %s", task.timeDimension == 0 ? "and hour=" + task.hour : "");
        try {
            jdbcTemplate.update(deleteSql, day, pubToken);
        } catch (Exception e) {
            return String.format("delete report_adtiming error,msg:%s", e.getMessage());
        }
        LOG.info("[Adtiming] jsonDataImportDatabase start,taskId:{}, day:{}", task.id, day);
        long start = System.currentTimeMillis();
        String error = "";
        String sql_insert = "INSERT INTO report_adtiming (day,hour,country,app_pk_name,app_name,app_key,os,placement_id,placement_name,request,filled,impression,click,video_start,video_finish,earning,pub_token) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        int count = 0;
        try {

            List<Object[]> lsParm = new ArrayList<>();
            JSONArray jsonArray = JSONObject.parseArray(jsonData);
            if (jsonArray.isEmpty())
                return "data is null";

            for (int i = 0; i < jsonArray.size(); i++) {
                JSONObject obj = jsonArray.getJSONObject(i);
                count++;
                Object[] params = new Object[]{obj.get("day"), obj.getIntValue("hour"),
                        countryService.convertA3ToA2(obj.getString("country")),
                        obj.getString("appId"), obj.getString("appName"),
                        obj.getString("appKey"), obj.getString("os"),
                        obj.getInteger("placementId"), obj.getString("placementName"),
                        obj.getLong("request"), obj.getLong("filled"),
                        obj.getLong("impr"), obj.getLong("click"),
                        obj.getLong("videoStart"), obj.getLong("videoFinish"),
                        obj.getBigDecimal("revenue"), pubToken
                };
                if (lsParm.size() >= 1000) {
                    jdbcTemplate.batchUpdate(sql_insert, lsParm);
                    lsParm = new ArrayList<>();
                }
                lsParm.add(params);
            }
            if (!lsParm.isEmpty()) {
                jdbcTemplate.batchUpdate(sql_insert, lsParm);
            }
        } catch (Exception e) {
            error = String.format("insert report_adtiming error, msg:%s", e.getMessage());
        }
        LOG.info("[Adtiming] jsonDataImportDatabase end, taskId:{}, count:{}, cost:{}", task.id, count,System.currentTimeMillis() - start);
        return error;
    }

    private String saveReportData(ReportTask task) {
        LOG.info("[Adtiming] saveReportData start, taskId:{}", task.id);
        long start = System.currentTimeMillis();
        String error;
        try {
            List<Map<String, Object>> instanceInfoList = getInstanceList(task.reportAccountId);
            if (instanceInfoList.isEmpty())
                return "instance is null";

            LocalDate dataDay = LocalDate.parse(task.day, DATEFORMAT_YMD);
            Map<String, Map<String, Object>> placements = new HashMap<>();
            for (Map<String, Object> ins : instanceInfoList) {
                String key = MapHelper.getString(ins, "adn_app_key") + "_" + MapHelper.getString(ins, "placement_key");
                putLinkKeyMap(placements, key, ins, dataDay);
            }

            String dataSql = "SELECT day,hour,country,concat(app_key,'_', placement_id) data_key," +
                    "sum(request) api_request, sum(filled) api_filled," +
                    "sum(impression) AS api_impr,sum(click) AS api_click," +
                    "sum(video_start) api_video_start,sum(video_finish) api_video_complete, sum(earning) AS revenue" +
                    " FROM report_adtiming WHERE day=? AND pub_token=? " +
                    " GROUP BY day,hour,country,placement_id";

            List<ReportAdnData> oriDataList = jdbcTemplate.query(dataSql, ReportAdnData.ROWMAPPER, task.day, task.adnAppToken);

            if (oriDataList.isEmpty())
                return "data is empty";

            error = toAdnetworkLinked(task, task.adnAppToken, placements, oriDataList);

        } catch (Exception e) {
            error = String.format("saveReportData error, msg:%s", e.getMessage());
        }
        LOG.info("[Adtiming] saveReportData end, taskId:{}, cost:{}", task.id, System.currentTimeMillis() - start);
        return error;
    }
}
