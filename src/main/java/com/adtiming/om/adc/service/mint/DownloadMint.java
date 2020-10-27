// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.adc.service.mint;

import com.adtiming.om.adc.dto.ReportAdnData;
import com.adtiming.om.adc.dto.ReportTask;
import com.adtiming.om.adc.service.AdnBaseService;
import com.adtiming.om.adc.service.AppConfig;
import com.adtiming.om.adc.util.MyHttpClient;
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
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.adtiming.om.adc.util.MapHelper.getString;

@Service
public class DownloadMint extends AdnBaseService {

    private static final Logger LOG = LogManager.getLogger();

    @Resource
    private JdbcTemplate jdbcTemplate;

    @Resource
    private AppConfig cfg;

    @Override
    public void setAdnInfo() {
        this.adnId = 18;
        this.adnName = "Mint";
    }

    @Override
    public void executeTaskImpl(ReportTask o) {
        try {
            executeMintTask(o);
        } catch (Exception e) {
            LOG.error("[{}] executeTaskImpl error, taskId:{}", this.adnName, o.id, e);
        }
    }

    private void executeMintTask(ReportTask task) {
        String token = task.adnAppToken;
        String day = task.day;
        // Set up AdSense Management API client.
        if (StringUtils.isBlank(token)) {
            LOG.error("[Mint] token is null, taskId:{}", task.id);
            return;
        }

        LOG.info("[Mint] executeTaskImpl start, taskId:{}", task.id);
        long start = System.currentTimeMillis();
        updateTaskStatus(jdbcTemplate, task.id, 1, "");
        String error;
        StringBuilder err = new StringBuilder();
        String json = downLoadData(task, token, day, err);
        if (StringUtils.isNoneBlank(json) && err.length() < 1) {
            task.step = 2;
            error = jsonDataImportDatabase(task, json, day, token);
            if (StringUtils.isBlank(error)) {
                task.step = 3;
                error = savePrepareReportData(task, day, token);
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
            LOG.error("[Mint] executeTaskImpl error,run count:{},taskId:{},msg:{}", task.runCount + 1, task.id, error);
        } else {
            updateTaskStatus(jdbcTemplate, task.id, status, error);
        }
        LOG.info("[Mint] executeTaskImpl end, taskId:{}, cost:{}", task.id, System.currentTimeMillis() - start);
    }

    private String downLoadData(ReportTask task, String token, String day, StringBuilder err) {
        String jsonData = "";
        HttpEntity entity = null;
        LOG.info("[Mint] downloadPageJsonData start, taskId:{}", task.id);
        long start = System.currentTimeMillis();
        try {
            LocalDate startLocalDate = LocalDate.parse(day, DateTimeFormatter.ofPattern("yyyy-MM-dd"));
            String startDate = startLocalDate.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
            String endDate = startLocalDate.plusDays(1).format(DateTimeFormatter.ofPattern("yyyyMMdd"));
            JSONObject param = new JSONObject();
            param.put("token", token);
            param.put("start", startDate);
            param.put("end", endDate);
            param.put("scale", "day");
            String[] metrics = new String[]{"request", "fill", "impression", "click", "revenue"};
            param.put("metrics", metrics);
            String[] breakdowns = new String[]{"app_id", "country", "tag_id", "platform", "date"};
            param.put("breakdowns", breakdowns);
            String paramsStr = JSON.toJSONString(param);
            String url = String.format("https://globalreport.ad.intl.xiaomi.com/max/report/colu/v1?%s", paramsStr);
            updateReqUrl(jdbcTemplate, task.id, url);
            HttpPost httpPost = new HttpPost("https://globalreport.ad.intl.xiaomi.com/max/report/colu/v1");
            StringEntity se = new StringEntity(paramsStr);
            RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(5 * 60 * 1000).setProxy(cfg.httpProxy).build();
            httpPost.setConfig(requestConfig);
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
            int code = obj.getInteger("code");
            if (code == 0) {
                return obj.getString("result");
            } else if (code == 10103) {
                err.append("data is null");
                return "";
            }
            err.append(jsonData);
            return "";
        } catch (Exception e) {
            err.append(String.format("downJsonData error, msg:%s", e.getMessage()));
        } finally {
            EntityUtils.consumeQuietly(entity);
        }
        LOG.info("[Mint] downloadPageJsonData end, taskId:{}, cost:{}", task.id, System.currentTimeMillis() - start);
        return jsonData;
    }

    private String jsonDataImportDatabase(ReportTask task, String jsonData, String day, String token) {
        String sql_delete = "delete from report_mint where day=? and app_key=? ";
        try {
            jdbcTemplate.update(sql_delete, day, token);
        } catch (Exception e) {
            return String.format("delete report_mint error,msg:%s", e.getMessage());
        }
        LOG.info("[Mint] jsonDataImportDatabase start, taskId:{}", task.id);
        long start = System.currentTimeMillis();
        String error = "";

        try {
            String sql_insert = "INSERT into report_mint (day, country, platform, app_id, tag_id, request, fill, impression, click, revenue, app_key) " +
                    " VALUES(?,?,?,?,?,?,?,?,?,?,?)";

            List<Object[]> lsParm = new ArrayList<>(1000);
            JSONArray jsonArray = JSONArray.parseArray(jsonData);
            for (int i = 0; i < jsonArray.size(); i++) {
                JSONObject obj = jsonArray.getJSONObject(i);
                Object[] params = new Object[]{day, obj.get("country"), obj.get("platform"), obj.get("app_id"), obj.get("tag_id"),
                        obj.get("request"), obj.get("fill"), obj.get("impression"), obj.get("click"), obj.get("revenue"), token};
                lsParm.add(params);
                if (lsParm.size() == 1000) {
                    jdbcTemplate.batchUpdate(sql_insert, lsParm);
                    lsParm.clear();
                }
            }
            if (!lsParm.isEmpty()) {
                jdbcTemplate.batchUpdate(sql_insert, lsParm);
            }
        } catch (Exception e) {
            error = String.format("insert report_mint error, msg:%s", e.getMessage());
        }
        LOG.info("[Mint] jsonDataImportDatabase end, taskId:{}, cost:{}", task.id, System.currentTimeMillis() - start);
        return error;
    }

    private String getAppKey(String key) {
        if (StringUtils.isBlank(key)) {
            return "";
        }
        if (key.contains("#")) {
            return key.split("#")[0];
        }
        return key;
    }

    private String savePrepareReportData(ReportTask task, String reportDay, String appKey) {
        LOG.info("[Mint] savePrepareReportData start, skey:{}, day:{}", appKey, reportDay);
        long start = System.currentTimeMillis();
        String error;
        try {
            List<Map<String, Object>> instanceInfoList = getInstanceList(task.reportAccountId);
            /*Map<String, Map<String, Object>> placements = instanceInfoList.stream().collect(Collectors.toMap(m -> getAppKey(getString(m,"adn_app_key")) + "_" + getString(m, "placement_key"), m -> m, (existingValue, newValue) -> existingValue));

            // instance's placement_key changed
            Set<Integer> insIds = instanceInfoList.stream().map(o-> getInt(o, "instance_id")).collect(Collectors.toSet());
            List<Map<String, Object>> oldInstanceList = getOldInstanceList(insIds);
            if (!oldInstanceList.isEmpty()) {
                placements.putAll(oldInstanceList.stream().collect(Collectors.toMap(m ->
                        getAppKey(getString(m,"adn_app_key")) + "_" + getString(m, "placement_key"), m -> m, (existingValue, newValue) -> existingValue)));
            }*/
            if (instanceInfoList.isEmpty()) {
                return "instance is null";
            }
            LocalDate dataDay = LocalDate.parse(task.day, DATEFORMAT_YMD);
            Map<String, Map<String, Object>> placements = new HashMap<>();
            for (Map<String, Object> ins : instanceInfoList) {
                String key = getAppKey(getString(ins,"adn_app_key")) + "_" + getString(ins, "placement_key");
                putLinkKeyMap(placements, key, ins, dataDay);
            }

            String dataSql = "select day,country,platform,concat(app_id,'_',tag_id) data_key,sum(request) api_request,sum(fill) AS api_filled," +
                    "sum(impression) api_impr,sum(click) AS api_click,sum(revenue) AS revenue" +
                    " from report_mint where day=? and app_key=? group by day,country,app_id,tag_id ";

            List<ReportAdnData> oriDataList = jdbcTemplate.query(dataSql, ReportAdnData.ROWMAPPER, reportDay, appKey);

            if (oriDataList.isEmpty())
                return "data is null";
            error = toAdnetworkLinked(task, appKey, placements, oriDataList);
        } catch (Exception e) {
            error = String.format("savePrepareReportData error, msg:%s", e.getMessage());
        }
        LOG.info("[Mint] savePrepareReportData end, taskId:{}, cost:{}", task.id, System.currentTimeMillis() - start);
        return error;
    }
}
