// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.adc.service.adcolony;

import com.adtiming.om.adc.dto.ReportAdnData;
import com.adtiming.om.adc.dto.ReportTask;
import com.adtiming.om.adc.service.AdnBaseService;
import com.adtiming.om.adc.service.AppConfig;
import com.adtiming.om.adc.util.DateTimeFormat;
import com.adtiming.om.adc.util.MyHttpClient;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
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
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.adtiming.om.adc.util.MapHelper.getInt;
import static com.adtiming.om.adc.util.MapHelper.getString;

@Service
public class DownloadAdcolony extends AdnBaseService {

    private static final Logger LOG = LogManager.getLogger();

    @Resource
    private JdbcTemplate jdbcTemplate;

    @Resource
    private AppConfig cfg;

    @Override
    public void setAdnInfo() {
        this.adnId = 7;
        this.adnName = "Adcolony";
    }

    @Override
    public void executeTaskImpl(ReportTask o) {
        try {
            executeAdcolonyTask(o);
        } catch (Exception e) {
            LOG.error("[{}] executeTaskImpl error, taskId:{}", this.adnName, o.id, e);
        }
    }

    private void executeAdcolonyTask(ReportTask task) {
        String appToken = task.adnAppToken;
        String day = task.day;
        if (StringUtils.isBlank(appToken)) {
            LOG.error("[Adcolony] appId or apiKey is null, taskId:{}", task.id);
            return;
        }

        LOG.info("[Adcolony] executeTaskImpl start, taskId:{}", task.id);
        long start = System.currentTimeMillis();
        updateTaskStatus(jdbcTemplate, task.id, 1, "");
        StringBuilder err = new StringBuilder();
        String error;
        String json_data = downJsonData(task.id, appToken, day, err);

        if (StringUtils.isNotBlank(json_data) && err.length() == 0) {
            error = jsonDataImportDatabase(task.id, json_data, day, appToken);
            if (StringUtils.isBlank(error)) {
                error = savePrepareReportData(task, day, appToken);
                if (StringUtils.isBlank(error))
                    error = reportLinkedToStat(task, appToken);
            }
        } else {
            error = err.toString();
        }
        int status = StringUtils.isBlank(error) || "data is null".equals(error) ? 2 : 3;
        if (task.runCount > 5 && status != 2) {
            LOG.error("[Adcolony] executeTaskImpl error,run count:{},taskId:{},msg:{}", task.runCount + 1, task.id, error);
        }
        updateTaskStatus(jdbcTemplate, task.id, status, error);

        LOG.info("[Adcolony] executeTaskImpl end, taskId:{}, cost:{}", task.id, System.currentTimeMillis() - start);
    }

    private String downJsonData(int taskId, String appToken, String day, StringBuilder err) {
        String json_data = "";
        HttpEntity entity = null;
        LOG.info("[Adcolony] downJsonData start, taskId:{}", taskId);
        long start = System.currentTimeMillis();
        try {
            LocalDate date = LocalDate.parse(day, DateTimeFormat.DAY_FORMAT);
            String startDate = DateTimeFormat.DAY_O_FORMAT.format(date);
            String endDate = DateTimeFormat.DAY_O_FORMAT.format(date);
            String url = String.format("http://clients-api.adcolony.com/api/v2/publisher_summary?user_credentials=%s&format=json&date_group=day&group_by=country&group_by=zone&date=%s&end_date=%s", appToken, startDate, endDate);
            LOG.info("[Adcolony] request url:{}", url);
            updateReqUrl(jdbcTemplate, taskId, url);
            HttpGet httpGet = new HttpGet(url);
            httpGet.setConfig(RequestConfig.custom().setSocketTimeout(5 * 60 * 1000).setCookieSpec(CookieSpecs.IGNORE_COOKIES).setProxy(cfg.httpProxy).build());
            //发送Post,并返回一个HttpResponse对象
            HttpResponse response = MyHttpClient.getInstance().execute(httpGet);
            StatusLine sl = response.getStatusLine();
            if (sl.getStatusCode() != 200) {//如果状态码为200,就是正常返回
                err.append(String.format("request report response statusCode:%d", sl.getStatusCode()));
                return json_data;
            }

            entity = response.getEntity();
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
        LOG.info("[Adcolony] downJsonData end, taskId:{}, cost:{}", taskId, appToken, System.currentTimeMillis() - start);
        return json_data;
    }

    private String jsonDataImportDatabase(int taskId, String jsonData, String day, String userToken) {
        try {
            String deleteSql = "delete from report_adcolony where day=? and user_token=?";
            jdbcTemplate.update(deleteSql, day, userToken);
        } catch (Exception e) {
            return String.format("delete report_adcolony error,msg:%s", e.getMessage());
        }
        LOG.info("[Adcolony] jsonDataImportDatabase start, taskId:{}", taskId);
        long start = System.currentTimeMillis();
        String error = "";

        String insertSql = "INSERT INTO report_adcolony (day,country,app_id,app_name,store_id,internal_zone_id," +
                "zone_id,zone_name,platform,impressions,house_impressions,cvvs,house_cvvs,clicks,earnings,ecpm,fill_rate," +
                "ctr,requests,completion_rate,user_token) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        try {
            int count = 0;
            List<Object[]> lsParm = new ArrayList<>();
            JSONObject jobj = JSONObject.parseObject(jsonData);
            if (StringUtils.isBlank(jobj.getString("results"))) {
                return "response results is null";
            }

            JSONArray jsonArray = JSONArray.parseArray(jobj.getString("results"));
            for (int i = 0; i < jsonArray.size(); i++) {
                JSONObject obj = jsonArray.getJSONObject(i);
                count++;
                Object[] params = new Object[]{obj.get("date"), obj.get("country"), obj.get("app_id"), obj.get("app_name"),
                        obj.get("store_id"), obj.get("internal_zone_id"), obj.get("zone_id"), obj.get("zone_name"), obj.get("platform"),
                        obj.get("impressions"), obj.get("house_impressions"), obj.get("cvvs"), obj.get("house_cvvs"), obj.get("clicks"),
                        obj.get("earnings") == null ? 0 : obj.get("earnings"), obj.get("ecpm") == null ? 0 : obj.get("ecpm"),
                        obj.get("fill_rate") == null ? 0 : obj.get("fill_rate"), obj.get("ctr") == null ? 0 : obj.get("ctr"),
                        obj.get("requests") == null ? 0 : obj.get("requests"),
                        obj.get("completion_rate") == null ? 0 : obj.get("completion_rate"), userToken};
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
            error = String.format("insert report_adcolony error, msg:%s", e.getMessage());
        }
        LOG.info("[Adcolony] jsonDataImportDatabase end, taskId:{}, cost:{}", taskId, System.currentTimeMillis() - start);
        return error;
    }

    private String savePrepareReportData(ReportTask task, String reportDay, String accountToken) {
        LOG.info("[Adcolony] savePrepareReportData start, taskId:{}", task.id);
        long start = System.currentTimeMillis();
        String error;
        try {
            String whereSql = String.format("b.client_secret='%s'", accountToken);
            List<Map<String, Object>> instanceInfoList = getInstanceList(whereSql);
            Map<String, Map<String, Object>> placements = instanceInfoList.stream().collect(Collectors.toMap(m ->
                    getString(m, "placement_key"), m -> m, (existingValue, newValue) -> existingValue));
            // instance's placement_key changed
            Set<Integer> insIds = instanceInfoList.stream().map(o-> getInt(o, "instance_id")).collect(Collectors.toSet());
            List<Map<String, Object>> oldInstanceList = getOldInstanceList(insIds);
            if (!oldInstanceList.isEmpty()) {
                placements.putAll(oldInstanceList.stream().collect(Collectors.toMap(m ->
                        getString(m, "placement_key"), m -> m, (existingValue, newValue) -> existingValue)));
            }

            String dataSql = "select day,country,platform,zone_id data_key," +
                    "sum(requests) api_request,sum(round(fill_rate * requests / 100)) api_filled,"+
                    "(sum(impressions) - sum(house_impressions)) api_impr,sum(clicks) api_click," +
                    "sum(case when completion_rate>0 then cvvs*100/completion_rate else 0 end) api_video_start," +
                    "sum(cvvs) api_video_complete,sum(earnings) AS revenue" +
                    " from report_adcolony " +
                    " where day=? and user_token=?" +
                    " group by day,country,platform,app_id,zone_id ";

            List<ReportAdnData> oriDataList = jdbcTemplate.query(dataSql, ReportAdnData.ROWMAPPER, reportDay, accountToken);

            if (oriDataList.isEmpty())
                return "data is empty";

            error = toAdnetworkLinked(task, accountToken, placements, oriDataList);

        } catch (Exception e) {
            error = String.format("savePrepareReportData error, msg:%s", e.getMessage());
        }
        LOG.info("[Adcolony] savePrepareReportData end, taskId:{},cost:{}", task.id, System.currentTimeMillis() - start);
        return error;
    }
}

