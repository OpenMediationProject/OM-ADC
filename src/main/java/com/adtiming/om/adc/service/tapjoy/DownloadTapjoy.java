// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.adc.service.tapjoy;

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
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
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

@Service
public class DownloadTapjoy extends AdnBaseService {

    private static final Logger LOG = LogManager.getLogger();

    private String baseUrl = "https://api.tapjoy.com/";

    @Resource
    private JdbcTemplate jdbcTemplate;

    @Resource
    private AppConfig cfg;

    @Override
    public void setAdnInfo() {
        this.adnId = 11;
        this.adnName = "Tapjoy";
    }

    @Override
    public void executeTaskImpl(ReportTask o) {
        try {
            executeTapjoyTask(o);
        } catch (Exception e) {
            LOG.error("[{}] executeTaskImpl error, taskId:{}", this.adnName, o.id, e);
        }
    }

    private void executeTapjoyTask(ReportTask o) {
        String appKey = o.adnApiKey;
        String authKey = o.adnAppToken;
        String day = o.day;
        if (StringUtils.isBlank(authKey)) {
            LOG.error("[Tapjoy]，authKey is null");
            return;
        }
        LOG.info("[Tapjoy] executeTaskImpl start, authKey:{}, day:{}", authKey, day);
        long start = System.currentTimeMillis();
        updateTaskStatus(jdbcTemplate, o.id, 1, "");
        StringBuilder err = new StringBuilder();
        String error;
        String json_data = downJsonData(o.id, authKey, day, err);
        if (StringUtils.isNotBlank(json_data) && err.length() == 0) {
            error = jsonDataImportDatabase(json_data, day, appKey);
            if (StringUtils.isBlank(error) && !error.equals("data is null")) {
                error = savePrepareReportData(o, day, appKey);
                if (StringUtils.isBlank(error)) {
                    error = reportLinkedToStat(o, appKey);
                }
            }
        } else {
            error = err.toString();
        }
        int status = StringUtils.isBlank(error) || "data is null".equals(error) ? 2 : 3;
        updateTaskStatus(jdbcTemplate, o.id, status, err.toString());
        LOG.info("[Tapjoy] executeTaskImpl end, authKey:{}, day:{}, cost:{}", authKey, day, System.currentTimeMillis() - start);
    }

    private String getToken(String authKey, String reportDay, StringBuilder err) {
        String token = "";
        String auth_url = baseUrl + "v1/oauth2/token";
        HttpEntity entity = null;
        try {
            HttpPost httpPost = new HttpPost(auth_url);
            httpPost.setHeader("Authorization", "Basic " + authKey);
            httpPost.setHeader("Accept", "application/json; */*");
            httpPost.setConfig(RequestConfig.custom().setCookieSpec(CookieSpecs.IGNORE_COOKIES).setProxy(cfg.httpProxy).build());
            //发送Post,并返回一个HttpResponse对象
            HttpResponse response = MyHttpClient.getInstance().execute(httpPost);
            entity = response.getEntity();

            StatusLine sl = response.getStatusLine();
            if (sl.getStatusCode() != 200) {//如果状态码为200,就是正常返回
                err.append(String.format("getToken response statusCode is %d", sl.getStatusCode()));
                return token;
            }

            entity = response.getEntity();
            if (entity == null) {
                err.append("getToken response entity is null");
                return token;
            }
            token = EntityUtils.toString(entity);
            JSONObject obj = JSONObject.parseObject(token);
            return obj.getString("access_token");
        } catch (Exception e) {

            LOG.error("[Tapjoy] getToken error, authKey:{},day:{}", authKey, reportDay, e);
        } finally {
            EntityUtils.consumeQuietly(entity);
        }
        return token;
    }

    private String downJsonData(int taskId, String authKey, String day, StringBuilder err) {
        String json_data = "";
        HttpEntity entity = null;
        String reportUrl = baseUrl + "v2/publisher/reports?date=" + day + "&page_size=100&mock=0&group_by=placements";
        String token = getToken(authKey, day, err);
        if (StringUtils.isBlank(token)) {
            LOG.error("[Tapjoy] getToken failed, taskId:{}, authKey:{}, day:{}", taskId, authKey, day);
            return json_data;
        }
        try {
            updateReqUrl(jdbcTemplate, taskId, reportUrl);
            LOG.info("[Tapjoy] downJsonData start, authKey:{}, day:{}", authKey, day);
            long start = System.currentTimeMillis();
            HttpGet httpGet = new HttpGet(reportUrl);
            httpGet.setHeader("Authorization", "Bearer " + token);
            httpGet.setHeader("Accept", "application/json; */*");
            RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(5 * 60 * 1000).setProxy(cfg.httpProxy).build();//设置请求和传输超时时间
            httpGet.setConfig(requestConfig);
            //发送Post,并返回一个HttpResponse对象
            HttpResponse response = MyHttpClient.getInstance().execute(httpGet);
            StatusLine sl = response.getStatusLine();
            LOG.info("Tapjoy:" + LocalDateTime.now().toLocalTime());
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
            LOG.info("[Tapjoy] downJsonData end, authKey:{}, day:{}, cost:{}", authKey, day, System.currentTimeMillis() - start);
            return json_data;
        } catch (Exception ex) {
            //log.error("[Tapjoy] downJsonData error, authKey:{}, day:{}", authKey, day);
            err.append(String.format("downJsonData error:%s", ex.getMessage()));
        } finally {
            EntityUtils.consumeQuietly(entity);
        }
        return "";
    }

    private String jsonDataImportDatabase(String jsonData, String day, String appKey) {
        String sql_delete = "DELETE FROM report_tapjoy WHERE day=? AND appKey=?";
        try {
            jdbcTemplate.update(sql_delete, day, appKey);
        } catch (Exception e) {
            return String.format("delete report_tapjoy error:%s", e.getMessage());
        }
        LOG.info("[Tapjoy] jsonDataImportDatabase start, appKey:{}, day:{}", appKey, day);
        long start = System.currentTimeMillis();
        String sql_insert = "INSERT INTO report_tapjoy (day, country, name, appkey, placement_name, platform, conversions, impressions, clicks, revenue, ecpm)  " +
                " VALUES(?,?,?,?,?,?,?,?,?,?,?)";
        String err = "";
        try {
            int count = 0;
            List<Object[]> lsParm = new ArrayList<>();
            JSONObject jobj = JSONObject.parseObject(jsonData);
            if (StringUtils.isBlank(jobj.getString("Apps")))
                return "data is null";

            JSONArray appArray = JSONArray.parseArray(jobj.getString("Apps"));
            for (int i = 0; i < appArray.size(); i++) {
                JSONObject app_obj = appArray.getJSONObject(i);
                JSONArray pArray = JSONArray.parseArray(app_obj.getString("Placements"));
                for (int j = 0; j < pArray.size(); j++) {
                    JSONObject p_obj = pArray.getJSONObject(j);
                    JSONArray jsonArray = JSONArray.parseArray(p_obj.getString("Countries"));
                    for (int k = 0; k < jsonArray.size(); k++) {
                        JSONObject obj = jsonArray.getJSONObject(k);
                        count++;
                        Object[] params = new Object[]{day, obj.get("Country"), app_obj.get("Name"), appKey, p_obj.get("Name"),
                                app_obj.get("Platform"), obj.get("Conversions"), obj.get("Impressions"), obj.get("Clicks"),
                                obj.get("Revenue") == null ? 0 : obj.get("Revenue"), obj.get("ECPM") == null ? 0 : obj.get("ECPM")};
                        if (count > 1000) {
                            jdbcTemplate.batchUpdate(sql_insert, lsParm);
                            count = 1;
                            lsParm = new ArrayList<>();
                        }
                        lsParm.add(params);
                    }
                }
            }
            if (!lsParm.isEmpty()) {
                jdbcTemplate.batchUpdate(sql_insert, lsParm);
            }
        } catch (Exception e) {
            err = String.format("insert report_tapjoy error:%s", e.getMessage());
        }
        LOG.info("[Tapjoy] jsonDataImportDatabase end, appKey:{}, day:{}, cost:{}", appKey, day, System.currentTimeMillis() - start);
        return err;
    }

    private String savePrepareReportData(ReportTask task, String reportDay, String appKey) {
        String error;
        LOG.info("[Tapjoy] savePrepareReportData start, taskId:{}", task.id);
        long start = System.currentTimeMillis();
        try {
            String whereSql = String.format("b.api_key='%s'", appKey);
            List<Map<String, Object>> instanceInfoList = getInstanceList(whereSql);
            Map<String, Map<String, Object>> placements = instanceInfoList.stream().collect(Collectors.toMap(m -> MapHelper.getString(m, "placement_key"), m -> m, (existingValue, newValue) -> existingValue));

            // instance's placement_key changed
            Set<Integer> insIds = instanceInfoList.stream().map(o -> MapHelper.getInt(o, "instance_id")).collect(Collectors.toSet());
            List<Map<String, Object>> oldInstanceList = getOldInstanceList(insIds);
            if (!oldInstanceList.isEmpty()) {
                placements.putAll(oldInstanceList.stream().collect(Collectors.toMap(m ->
                        MapHelper.getString(m, "placement_key"), m -> m, (existingValue, newValue) -> existingValue)));
            }

            String dataSql = "SELECT day,country,platform,placement_name data_key," +
                    "sum(impressions) AS api_impr,sum(clicks) AS impr_click,sum(revenue) AS revenue" +
                    " FROM report_tapjoy WHERE day=? AND appKey=? GROUP BY day,country,placement_name,platform ";

            List<ReportAdnData> oriDataList = jdbcTemplate.query(dataSql, ReportAdnData.ROWMAPPER, reportDay, appKey);

            if (oriDataList.isEmpty())
                return "data is null";
            error = toAdnetworkLinked(task, appKey, placements, oriDataList);
        } catch (Exception e) {
            error = String.format("savePrepareReportData error:%s", e.getMessage());
        }
        LOG.info("[Tapjoy] savePrepareReportData end, taskId:{}, cost:{}", task.id, System.currentTimeMillis() - start);
        return error;
    }
}
