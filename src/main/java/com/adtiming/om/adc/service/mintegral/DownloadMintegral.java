// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.adc.service.mintegral;

import com.adtiming.om.adc.dto.ReportAdnData;
import com.adtiming.om.adc.dto.ReportTask;
import com.adtiming.om.adc.service.AdnBaseService;
import com.adtiming.om.adc.service.AppConfig;
import com.adtiming.om.adc.util.Encrypter;
import com.adtiming.om.adc.util.MyHttpClient;
import com.adtiming.om.adc.util.ParamsBuilder;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.StatusLine;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.*;
import java.util.stream.Collectors;

import static com.adtiming.om.adc.util.MapHelper.getInt;
import static com.adtiming.om.adc.util.MapHelper.getString;

@Service
public class DownloadMintegral extends AdnBaseService {

    private static final Logger LOG = LogManager.getLogger();

    @Resource
    private JdbcTemplate jdbcTemplate;

    @Resource
    private AppConfig cfg;

    @Override
    public void setAdnInfo() {
        this.adnId = 14;
        this.adnName = "Mintegral";
    }

    @Override
    public void executeTaskImpl(ReportTask o) {
        try {
            executeMintegralTask(o);
        } catch (Exception e) {
            LOG.error("[{}] executeTaskImpl error, taskId:{}", this.adnName, o.id, e);
        }
    }

    private void executeMintegralTask(ReportTask task) {
        String skey = task.adnApiKey;
        String secret = task.userSignature;
        String day = task.day;
        // Set up AdSense Management API client.
        if (StringUtils.isBlank(skey)) {
            LOG.error("[Mintegral] skey is null, taskId:{}", task.id);
            return;
        }
        if (StringUtils.isBlank(secret)) {
            LOG.error("[Mintegral] secret is null, taskId:{}", task.id);
            return;
        }

        LOG.info("[Mintegral] executeTaskImpl start, skey:{}, secret:{}, day:{}", skey, secret, day);
        long start = System.currentTimeMillis();
        updateTaskStatus(jdbcTemplate, task.id, 1, "");
        String error = downLoadData(task, skey, secret, day, day);
        if (StringUtils.isBlank(error)) {
            task.step = 3;
            error = savePrepareReportData(task, day, skey);
            if (StringUtils.isBlank(error)) {
                task.step = 4;
                error = reportLinkedToStat(task, skey);
            }
        }

        int status = getStatus(error);
        error = convertMsg(error);
        if (task.runCount >= 4 && status != 2) {
            updateAccountException(jdbcTemplate, task, error);
            LOG.error("[Mintegral] executeTaskImpl error,run count:{},taskId:{},msg:{}", task.runCount + 1, task.id, error);
        } else {
            updateTaskStatus(jdbcTemplate, task.id, status, error);
        }
        LOG.info("[Mintegral] executeTaskImpl end, skey:{}, secret:{}, day:{}, cost:{}", skey, secret, day, System.currentTimeMillis() - start);
    }

    private String downLoadData(ReportTask task, String skey, String secret, String startDate, String endDate) {
        String error = "";
        StringBuilder err = new StringBuilder();
        try {
            task.step = 1;
            String jsonData = downloadPageJsonData(task.id, skey, secret, startDate, endDate, 0, err);
            if (StringUtils.isNotBlank(jsonData) && err.length() == 0) {
                JSONObject jsonObject = JSONObject.parseObject(jsonData);
                if ("ok".equals(jsonObject.getString("code").toLowerCase())) {
                    JSONObject obj = jsonObject.getJSONObject("data");
                    err.append(jsonDataImportDatabase(task, obj.getString("lists"), startDate, skey, 0));
                    int total_page = obj.getIntValue("total_page");
                    for (int i = 1; i < total_page; i++) {
                        jsonData = downloadPageJsonData(task.id, skey, secret, startDate, endDate, i, err);
                        if (StringUtils.isNotBlank(jsonData) && err.length() == 0) {
                            JSONObject sObj = JSONObject.parseObject(jsonData);
                            err.append(jsonDataImportDatabase(task, sObj.getJSONObject("data").getString("lists"), startDate, skey, i));
                        }
                    }
                    if (err.length() > 0) {
                        error = err.toString();
                    }
                } else {
                    error = "response code is not ok, res:" + jsonData;
                }
            } else {
                error = "response json data is null";
            }
        } catch (Exception e) {
            error = String.format("downJsonData error,msg:%s", e.getMessage());
        }
        return error;
    }

    private String downloadPageJsonData(int taskId, String skey, String secret, String startDate, String endDate, int page, StringBuilder err) {
        String jsonData = "";
        HttpEntity entity = null;
        LOG.info("[Mintegral] downloadPageJsonData start, skey:{}, secret:{}, day:{}, page:{}", skey, secret, startDate, page);
        long start = System.currentTimeMillis();
        try {
            String groupBy = "date,country,app_id,platform,unit_id";
            String params = buildMintegralParams(skey, secret, groupBy, page, StringUtils.replace(endDate, "-", ""), StringUtils.replace(startDate, "-", ""));
            String url = String.format("https://api.mintegral.com/reporting/data?%s", params);
            LOG.info("[Mintegral] request url:{}", url);
            updateReqUrl(jdbcTemplate, taskId, url);
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
            return jsonData;
        } catch (Exception e) {
            err.append(String.format("downJsonData error, msg:%s", e.getMessage()));
        } finally {
            EntityUtils.consumeQuietly(entity);
        }
        LOG.info("[Mintegral] downloadPageJsonData end, skey:{}, secret:{}, day:{}, page:{} cost:{}", skey, secret, startDate, page, System.currentTimeMillis() - start);
        return jsonData;
    }

    private String buildMintegralParams(String skey, String secret, String group_by, int page, String start, String end) {
        String currentTime = String.format("%010d", new Date().getTime() / 1000);
        ParamsBuilder pb = new ParamsBuilder()
                .p("limit", "1000")
                .p("group_by", group_by)
                .p("skey", skey)
                .p("page",page)
                .p("start", start)
                .p("end", end)
                .p("time", currentTime);
        pb.params().sort(Comparator.comparing(NameValuePair::getName));
        String str = pb.format();
        String sign = Encrypter.md5(Encrypter.md5(str) + secret);
        pb.p("sign", sign);
        return pb.format();
    }

    private String jsonDataImportDatabase(ReportTask task, String jsonData, String day, String appKey, int page) {
        task.step = 2;
        if (page == 0) {
            String sql_delete = "delete from report_mintegral where day=? and app_key=? ";
            try {
                jdbcTemplate.update(sql_delete, day, appKey);
            } catch (Exception e) {
                return String.format("delete report_mintegral error,msg:%s", e.getMessage());
            }
        }
        LOG.info("[Mintegral] jsonDataImportDatabase start, skey:{}, day:{}, page:{}", appKey, day, page);
        long start = System.currentTimeMillis();
        String error = "";

        try {
            String sql_insert = "INSERT into report_mintegral (day, country, ad_format, app_id, app_name, unit_id, unit_name, platform, " +
                    "request, filled, fill_rate, impression, click, revenue, ecpm, ctr, app_key) " +
                    " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

            List<Object[]> lsParm = new ArrayList<>(1000);
            JSONArray jsonArray = JSONArray.parseArray(jsonData);
            for (int i = 0; i < jsonArray.size(); i++) {
                JSONObject obj = jsonArray.getJSONObject(i);
                Object[] params = new Object[]{day, obj.get("country"), obj.get("ad_format"), obj.get("app_id"), obj.get("app_name"),
                        obj.get("unit_id"), obj.get("unit_name"), obj.get("platform"), obj.get("request"), obj.get("filled"),
                        obj.get("fill_rate") == null ? 0 : obj.get("fill_rate"), obj.get("impression"), obj.get("click"),
                        obj.get("est_revenue") == null ? 0 : obj.get("est_revenue"), obj.get("ecpm") == null ? 0 : obj.get("ecpm"),
                        obj.get("ctr") == null ? 0 : obj.get("ctr"), appKey};
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
            error = String.format("insert report_mintegral error, msg:%s", e.getMessage());
        }
        LOG.info("[Mintegral] jsonDataImportDatabase end, skey:{}, day:{}, page:{}, cost:{}", appKey, day, page, System.currentTimeMillis() - start);
        return error;
    }

    private String savePrepareReportData(ReportTask task, String reportDay, String appKey) {
        LOG.info("[Mintegral] savePrepareReportData start, skey:{}, day:{}", appKey, reportDay);
        long start = System.currentTimeMillis();
        String error;
        try {
            List<Map<String, Object>> instanceInfoList = getInstanceList(task.reportAccountId);
            Map<String, Map<String, Object>> placements = instanceInfoList.stream().collect(Collectors.toMap(m -> getString(m, "placement_key"), m -> m, (existingValue, newValue) -> existingValue));

            // instance's placement_key changed
            Set<Integer> insIds = instanceInfoList.stream().map(o-> getInt(o, "instance_id")).collect(Collectors.toSet());
            List<Map<String, Object>> oldInstanceList = getOldInstanceList(insIds);
            if (!oldInstanceList.isEmpty()) {
                placements.putAll(oldInstanceList.stream().collect(Collectors.toMap(m ->
                        getString(m, "placement_key"), m -> m, (existingValue, newValue) -> existingValue)));
            }

            String dataSql = "select day,country,platform,unit_id data_key,sum(request) api_request,sum(filled) AS api_filled," +
                    "sum(impression) api_impr,sum(click) AS api_click,sum(revenue) AS revenue" +
                    " from report_mintegral where day=? and app_key=? group by day,country,unit_id ";

            List<ReportAdnData> oriDataList = jdbcTemplate.query(dataSql, ReportAdnData.ROWMAPPER, reportDay, appKey);

            if (oriDataList.isEmpty())
                return "data is null";
            error = toAdnetworkLinked(task, appKey, placements, oriDataList);
        } catch (Exception e) {
            error = String.format("savePrepareReportData error, msg:%s", e.getMessage());
        }
        LOG.info("[Mintegral] savePrepareReportData end, taskId:{}, cost:{}", task.id, System.currentTimeMillis() - start);
        return error;
    }
}
