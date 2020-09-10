// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.adc.service.vungle;

import com.adtiming.om.adc.dto.ReportAdnData;
import com.adtiming.om.adc.dto.ReportTask;
import com.adtiming.om.adc.service.AdnBaseService;
import com.adtiming.om.adc.service.AppConfig;
import com.adtiming.om.adc.util.MapHelper;
import com.adtiming.om.adc.util.MyHttpClient;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import io.micrometer.core.instrument.util.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class DownloadVungle extends AdnBaseService {

    private static final Logger LOG = LogManager.getLogger();

    @Resource
    private JdbcTemplate jdbcTemplate;

    @Resource
    private AppConfig cfg;

    @Override
    public void setAdnInfo() {
        this.adnId = 5;
        this.adnName = "Vungle";
    }

    @Override
    public void executeTaskImpl(ReportTask o) {
        try {
            executeVungleTask(o);
        } catch (Exception e) {
            LOG.error("[{}] executeTaskImpl error, taskId:{}", this.adnName, o.id);
        }
    }

    private void executeVungleTask(ReportTask task) {
        String appKey = task.adnApiKey;
        String day = task.day;
        if (StringUtils.isBlank(appKey)) {
            LOG.error("[Vungle]，appKey is null");
            return;
        }
        LOG.info("[Vungle] executeTaskImpl start, apiKey:{}, day:{}", appKey, day);
        long start = System.currentTimeMillis();
        updateTaskStatus(jdbcTemplate, task.id, 1, "");
        StringBuilder err = new StringBuilder();
        String error;
        task.step = 1;
        String jsonData = downJsonData(task.id, appKey, day, err);
        if (StringUtils.isNotBlank(jsonData) && err.length() == 0) {
            task.step = 2;
            error = jsonDataImportDatabase(jsonData, day, appKey);
            if (StringUtils.isBlank(error)) {
                task.step = 3;
                error = savePrepareReportData(task, day, appKey);
                if (StringUtils.isBlank(error)) {
                    task.step = 4;
                    error = reportLinkedToStat(task, appKey);
                }
            }
        } else {
            error = err.toString();
        }
        int status = getStatus(error);
        error = convertMsg(error);
        if (task.runCount >= 4 && status != 2) {
            updateAccountException(jdbcTemplate, task, error);
            LOG.error("[Vungle] executeTaskImpl error,run count:{},taskId:{},msg:{}", task.runCount + 1, task.id, error);
        } else {
            updateTaskStatus(jdbcTemplate, task.id, status, error);
        }
        LOG.info("[Vungle] executeTaskImpl end, authKey:{}, day:{}, cost:{}", appKey, day, System.currentTimeMillis() - start);
    }

    private String downJsonData(int taskId, String apiKey, String day, StringBuilder err) {
        String jsonData = "";
        HttpEntity entity = null;
        try {
            LOG.info("[Vungle] downJsonData start, taskId:{}, apiKey:{}, day:{}", taskId, apiKey, day);
            long start = System.currentTimeMillis();
            String url = "https://report.api.vungle.com/ext/pub/reports/performance?" +
                    "dimensions=date,country,platform,application,application,placement&aggregates=views,completes,clicks,revenue,ecpm" +
                    "&start=" + day + "&end=" + day;
            updateReqUrl(jdbcTemplate, taskId, url);
            HttpGet httpGet = new HttpGet(url);
            httpGet.setConfig(RequestConfig.custom().setCookieSpec(CookieSpecs.IGNORE_COOKIES).setProxy(cfg.httpProxy).build());
            httpGet.setHeader("Authorization", "Bearer" + apiKey + "");
            httpGet.setHeader("Vungle-Version", "1");
            httpGet.setHeader("Accept", "application/json");
            HttpResponse response = MyHttpClient.getInstance().execute(httpGet);
            StatusLine sl = response.getStatusLine();
            entity = response.getEntity();
            if (sl.getStatusCode() != 200) {//如果状态码为200,就是正常返回
                err.append(String.format("request report response statusCode:%d,msg:%s", sl.getStatusCode(), entity == null ? "" : EntityUtils.toString(entity)));
                return jsonData;
            }

            if (entity == null) {
                err.append("request report response enity is null");
                return jsonData;
            }
            jsonData = EntityUtils.toString(entity);
            if (StringUtils.isBlank(jsonData)) {
                err.append("response data is null");
            }
            LOG.info("[Vungle] downJsonData end, taskId:{}, apiKey:{}, day:{}, cost:{}", taskId, apiKey, day, System.currentTimeMillis() - start);
            return jsonData;
        } catch (Exception ex) {
            err.append(String.format("downJsonData error:%s", ex.getMessage()));
        } finally {
            EntityUtils.consumeQuietly(entity);
        }
        return jsonData;
    }

    private String jsonDataImportDatabase(String jsonData, String day, String apiKey) {

        String sql_delete = "DELETE FROM report_vungle WHERE date(day)=? AND app_key=?";
        try {
            jdbcTemplate.update(sql_delete, day, apiKey);
        } catch (Exception e) {
            return String.format("delete report_vungle error:%s", e.getMessage());
        }

        LOG.info("[Vungle] jsonDataImportDatabase start, apiKey:{}, day:{}", apiKey, day);
        long start = System.currentTimeMillis();
        String err = "";
        int count = 0;
        try {
            String sql_insert = "INSERT into report_vungle (day,country,platform,application_id,application_name,placement_id," +
                    "placement_reference_id,placement_name,views,completes,clicks,revenue,ecpm,app_key)  VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
            List<Object[]> lsParm = new ArrayList<>();
            JSONArray jsonArray = JSONArray.parseArray(jsonData);
            for (int i = 0; i < jsonArray.size(); i++) {
                JSONObject obj = jsonArray.getJSONObject(i);
                count++;
                Object[] params = new Object[]{obj.get("date"), obj.get("country"), obj.get("platform"), obj.get("application id"),
                        obj.get("application name"), obj.get("placement id"), obj.get("placement reference id"), obj.get("placement name"),
                        obj.get("views"), obj.get("completes"), obj.get("clicks"), obj.get("revenue") == null ? 0 : obj.get("revenue"),
                        obj.get("ecpm") == null ? 0 : obj.get("ecpm"), apiKey};
                if (lsParm.size() > 1000) {
                    jdbcTemplate.batchUpdate(sql_insert, lsParm);
                    lsParm = new ArrayList<>();
                }
                lsParm.add(params);
            }
            if (!lsParm.isEmpty()) {
                jdbcTemplate.batchUpdate(sql_insert, lsParm);
            }
        } catch (Exception e) {
            err = String.format("insert report_vungle error:%s", e.getMessage());
            //log.error("[Vungle] insert report_vungle error, apiKey:{}, day:{}", apiKey, day, e);
        }
        LOG.info("[Vungle] jsonDataImportDatabase end, apiKey:{}, day:{}, insert count:{}, cost:{}",
                apiKey, day, count, System.currentTimeMillis() - start);
        return err;
    }


    private String savePrepareReportData(ReportTask task, String reportDay, String appKey) {
        String err;
        LOG.info("[Vungle] savePrepareReportData start, taskId:{}", task.id);
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

            String dataSql = "select day,country,platform,placement_reference_id dataKey," +
                    "sum(views) api_video_start,sum(completes) AS api_video_complete," +
                    "sum(views) AS api_impr,sum(clicks) AS api_click," +
                    "sum(revenue) AS revenue " +
                    " from report_vungle" +
                    " where day=? and app_key=?" +
                    " group by country,platform,placement_reference_id";

            List<ReportAdnData> oriDataList = jdbcTemplate.query(dataSql, ReportAdnData.ROWMAPPER, reportDay, appKey);

            if (oriDataList.isEmpty())
                return "data is empty";

            err = toAdnetworkLinked(task, appKey, placements, oriDataList);
        } catch (Exception e) {
            err = String.format("savePrepareReportData error:%s", e.getMessage());
        }
        LOG.info("[Vungle] savePrepareReportData end, taskId:{}, cost:{}", task.id, System.currentTimeMillis() - start);
        return err;
    }
}
