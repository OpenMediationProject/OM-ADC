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
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.adtiming.om.adc.util.MapHelper.getInt;

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
        String json_data = downJsonData(task.id, token, day, err);
        if (StringUtils.isNotBlank(json_data) && err.length() == 0) {
            error = jsonDataImportDatabase(json_data, day, token);
            if (StringUtils.isBlank(error)) {
                error = saveReportData(task);
                if (StringUtils.isBlank(error))
                    error = reportLinkedToStat(task, token);
            }
        } else {
            error = err.toString();
        }
        int status = StringUtils.isBlank(error) || "data is null".equals(error) ? 2 : 3;
        if (task.runCount > 5 && status != 2) {
            LOG.error("[Adtiming] executeTaskImpl error,run count:{},taskId:{},msg:{}", task.runCount + 1, task.id, error);
        }
        updateTaskStatus(jdbcTemplate, task.id, status, error);

        LOG.info("[Adtiming] executeTaskImpl end, pubToken:{}, cost:{}", token, day, System.currentTimeMillis() - start);
    }

    private String downJsonData(int taskId, String pubToken, String day, StringBuilder err) {
        String url = String.format("http://sdk.adtimingapi.com/report?type=json&encoding=UTF-8&token=%s&mediation=100&df=%s&dt=%s", pubToken, day, day);
        String json_data = "";
        HttpEntity entity = null;
        LOG.info("[Adtiming] request url:{}", url);
        LOG.info("[Adtiming] downJsonData start, taskId:{}", taskId);
        long start = System.currentTimeMillis();
        try {
            updateReqUrl(jdbcTemplate, taskId, url);
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
        LOG.info("[Adtiming] downJsonData end, taskId:{}, cost:{}", taskId, System.currentTimeMillis() - start);
        return json_data;
    }

    private String jsonDataImportDatabase(String jsonData, String day, String pubToken) {
        String deleteSql = "DELETE FROM report_adtiming WHERE day=? AND pub_token=?";
        try {
            jdbcTemplate.update(deleteSql, day, pubToken);
        } catch (Exception e) {
            return String.format("delete report_adtiming error,msg:%s", e.getMessage());
        }
        LOG.info("[Adtiming] jsonDataImportDatabase start,pubToken:{}, day:{}", pubToken, day);
        long start = System.currentTimeMillis();
        String error = "";
        String sql_insert = "INSERT INTO report_adtiming (day,hour,country,app_id,app_name,placement_id,placement_name," +
                "impression,click,earning,ecpm,pub_token) " +
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?)";
        int count = 0;
        try {

            List<Object[]> lsParm = new ArrayList<>();
            JSONArray jsonArray = JSONObject.parseArray(jsonData);
            if (jsonArray.isEmpty())
                return "data is null";

            for (int i = 0; i < jsonArray.size(); i++) {
                JSONObject obj = jsonArray.getJSONObject(i);
                count++;
                Object[] params = new Object[]{obj.get("Date"), 0,
                        countryService.convertA3ToA2(obj.getString("Country")),
                        obj.getInteger("AppID"), obj.getString("AppName"),
                        obj.getInteger("PlacementId"), obj.getString("PlacementName"),
                        obj.getLong("Impression"), obj.getLong("Click"),
                        obj.getBigDecimal("Earning"), obj.getBigDecimal("Ecpm"), pubToken
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
        LOG.info("[Adtiming] jsonDataImportDatabase end, pubToken:{}, day:{}, count:{}, cost:{}", pubToken, day,
                count,System.currentTimeMillis() - start);
        return error;
    }

    private String saveReportData(ReportTask task) {
        LOG.info("[Adtiming] saveReportData start, taskId:{}", task.id);
        long start = System.currentTimeMillis();
        String error;
        try {
            String whereSql = String.format("b.refresh_token='%s'", task.adnAppToken);
            String changeSql = String.format("(b.refresh_token='%s' or b.new_account_key='%s')", task.adnAppToken, task.adnAppToken);
            List<Map<String, Object>> instanceInfoList = getInstanceList(whereSql, changeSql);

            Map<String, Map<String, Object>> placements = instanceInfoList.stream().collect(Collectors.toMap(m ->
                    MapHelper.getString(m, "placement_key"), m -> m, (existingValue, newValue) -> existingValue));

            // instance's placement_key changed
            Set<Integer> insIds = instanceInfoList.stream().map(o -> getInt(o, "instance_id")).collect(Collectors.toSet());
            List<Map<String, Object>> oldInstanceList = getOldInstanceList(insIds);
            if (!oldInstanceList.isEmpty()) {
                placements.putAll(oldInstanceList.stream().collect(Collectors.toMap(m ->
                        MapHelper.getString(m, "placement_key"), m -> m, (existingValue, newValue) -> existingValue)));
            }

            String dataSql = "SELECT day,hour,country,placement_id data_key," +
                    "sum(impression) AS api_impr,sum(click) AS api_click,sum(earning) AS revenue" +
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
