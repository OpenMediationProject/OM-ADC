// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.adc.service.tiktok;

import com.adtiming.om.adc.dto.ReportAdnData;
import com.adtiming.om.adc.dto.ReportTask;
import com.adtiming.om.adc.service.AdnBaseService;
import com.adtiming.om.adc.service.AppConfig;
import com.adtiming.om.adc.util.MyHttpClient;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.codec.digest.DigestUtils;
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
import java.util.*;
import java.util.stream.Collectors;

import static com.adtiming.om.adc.util.MapHelper.getInt;
import static com.adtiming.om.adc.util.MapHelper.getString;

@Service
public class DownloadTikTok extends AdnBaseService {

    private static final Logger LOG = LogManager.getLogger();

    @Resource
    private JdbcTemplate jdbcTemplate;

    @Resource
    private AppConfig cfg;

    @Override
    public void setAdnInfo() {
        this.adnId = 13;
        this.adnName = "TikTok";
    }

    @Override
    public void executeTaskImpl(ReportTask o) {
        try {
            executeTikTokTask(o);
        } catch (Exception e) {
            LOG.error("[{}] executeTaskImpl error, taskId:{}", this.adnName, o.id, e);
        }
    }

    private void executeTikTokTask(ReportTask task) {
        String userId = task.userId;
        String userSecret = task.userSignature;
        String day = task.day;
        // Set up AdSense Management API client.
        if (StringUtils.isBlank(userId) || StringUtils.isBlank(userSecret)) {
            LOG.error("[TikTok] executeTikTokTask error, userId or userSignature is null");
            return;
        }

        LOG.info("[TikTok] executeTaskImpl start, userId:{}, userSign:{}, day:{}", userId, userSecret, day);
        long start = System.currentTimeMillis();
        updateTaskStatus(jdbcTemplate, task.id, 1, "");
        StringBuilder err = new StringBuilder();
        String error;

        String json_data = downJsonData(task.id, userId, userSecret, day, err);
        if (StringUtils.isNotBlank(json_data) && err.length() == 0) {
            error = jsonDataImportDatabase(json_data, day, userId, userSecret);
            if (StringUtils.isBlank(error)) {
                error = savePrepareReportData(task, day, userSecret);
                if (StringUtils.isBlank(error))
                    error = reportLinkedToStat(task, userSecret);
            }
        } else {
            error = err.toString();
        }

        int status = StringUtils.isBlank(error) || "data is null".equals(error) ? 2 : 3;
        if (task.runCount > 5 && status != 2) {
            LOG.error("[TikTok] executeTaskImpl error,run count:{},taskId:{},msg:{}", task.runCount + 1, task.id, error);
        }
        updateTaskStatus(jdbcTemplate, task.id, status, error);

        LOG.info("[TikTok] executeTaskImpl end, userId:{}, userSign:{}, day:{}, cost:{}", userId, userSecret, day, System.currentTimeMillis() - start);
    }

    private String downJsonData(int taskId, String userId, String userSign, String day, StringBuilder err) {
        String jsonData = "";
        HttpEntity entity = null;
        LOG.info("[TikTok] downJsonData start, taskId:{}, userId:{}, userSign:{}, day:{}", taskId, userId, userSign, day);
        long start = System.currentTimeMillis();
        try {
            Random ra = new Random();
            int nonce = ra.nextInt(999) + 1;
            long currentTime = new Date().getTime();
            String url = "http://ad.oceanengine.com/union/media/open/api/report/slot?time_granularity=STAT_TIME_GRANULARITY_DAILY&user_id=" + userId + "&sign=" + tikTokSign(userSign, nonce, currentTime) +
                    "&nonce=" + nonce + "&timestamp=" + currentTime + "&start_date=" + day + "&end_date=" + day;
            LOG.info("[TikTok] request url:{}", url);
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
        } catch (Exception ex) {
            err.append(String.format("downJsonData error,msg:%s", ex.getMessage()));
        } finally {
            EntityUtils.consumeQuietly(entity);
        }
        LOG.info("[TikTok] downJsonData end, taskId:{}, userId:{}, userSign:{}, day:{}, cost:{}", taskId, userId, userSign, day, System.currentTimeMillis() - start);
        return jsonData;
    }

    private String tikTokSign(String secure_key, Integer nonce, long currentTime) {
        String[] arr = new String[]{String.valueOf(currentTime), String.valueOf(nonce), secure_key};
        Arrays.sort(arr);
        String sign = StringUtils.join(arr, "");
        return DigestUtils.sha1Hex(sign);
    }

    private String jsonDataImportDatabase(String jsonData, String day, String userId, String userSign) {
        LOG.info("[TikTok] jsonDataImportDatabase start, userId:{}, userSign:{}, day:{}", userId, userSign, day);
        long start = System.currentTimeMillis();
        String error = "";

        String insertSql = "INSERT INTO report_tiktok (day, region, appid, ad_slot_id,ad_slot_type,site_name,code_name,currency,media_name,impressions, click, click_rate, cost, ecpm, secure_key) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        try {
            int count = 0;
            List<Object[]> lsParm = new ArrayList<>();
            JSONObject jobj = JSONObject.parseObject(jsonData);
            if (StringUtils.isBlank(jobj.getString("data"))) {
                String msg = jobj.getString("message");
                if (StringUtils.isBlank(msg)) {
                    msg = "response data is null";
                }
                return msg;
            }
            String deleteSql = "DELETE FROM report_tiktok WHERE day=? AND secure_key=?";
            try {
                jdbcTemplate.update(deleteSql, day, userSign);
            } catch (Exception e) {
                return String.format("delete report_tiktok error,msg:%s", e.getMessage());
            }
            JSONArray jsonArray = JSONArray.parseArray(jobj.getString("data"));
            for (int i = 0; i < jsonArray.size(); i++) {
                JSONObject obj = jsonArray.getJSONObject(i);
                count++;
                Object[] params = new Object[]{obj.get("stat_datetime"), obj.get("region"), obj.get("appid"), obj.get("ad_slot_id"), obj.getString("ad_slot_type"),
                        obj.getString("site_name"), obj.getString("code_name"), obj.getString("currency"), obj.getString("media_name"),
                        obj.get("show") == null ? 0 : obj.get("show"), obj.get("click") == null ? 0 : obj.get("click"),
                        obj.get("click_rate") == null ? 0 : obj.get("click_rate"), obj.get("cost") == null ? 0 : obj.get("cost"),
                        obj.get("ecpm") == null ? 0 : obj.get("ecpm"), userSign};
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
            error = String.format("insert report_tiktok error, msg:%s", e.getMessage());
        }
        LOG.info("[TikTok] jsonDataImportDatabase end, userId:{}, userSign:{}, day:{}, cost:{}", userId, userSign, day, System.currentTimeMillis() - start);
        return error;
    }

    private String savePrepareReportData(ReportTask task, String reportDay, String secureKey) {
        LOG.info("[TikTok] savePrepareReportData start, taskId:{}", task.id);
        long start = System.currentTimeMillis();
        String error;
        try {
            String whereSql = String.format("b.client_secret='%s'", secureKey);
            String changeSql = String.format("(b.client_secret='%s' or b.new_account_key='%s')", secureKey, secureKey);
            List<Map<String, Object>> instanceInfoList = getInstanceList(whereSql, changeSql);
            Map<String, Map<String, Object>> placements = instanceInfoList.stream().collect(Collectors.toMap(m -> getString(m, "placement_key"), m -> m, (existingValue, newValue) -> existingValue));

            // instance's placement_key changed
            Set<Integer> insIds = instanceInfoList.stream().map(o-> getInt(o, "instance_id")).collect(Collectors.toSet());
            List<Map<String, Object>> oldInstanceList = getOldInstanceList(insIds);
            if (!oldInstanceList.isEmpty()) {
                placements.putAll(oldInstanceList.stream().collect(Collectors.toMap(m ->
                        getString(m, "placement_key"), m -> m, (existingValue, newValue) -> existingValue)));
            }

            String dataSql = "select day,region country,platform,ad_slot_id data_key," +
                    "sum(impressions) api_impr,sum(click) api_click,sum(cost) AS revenue " +
                    "  from report_tiktok where day=? and secure_key=? group by day,region,ad_slot_id ";

            List<ReportAdnData> oriDataList = jdbcTemplate.query(dataSql, ReportAdnData.ROWMAPPER, reportDay, secureKey);

            if (oriDataList.isEmpty())
                return "data is null";
            error = toAdnetworkLinked(task, secureKey, placements, oriDataList);
        } catch (Exception e) {
            error = String.format("savePrepareReportData error, msg:%s", e.getMessage());
        }
        LOG.info("[TikTok] savePrepareReportData end, taskId:{}, cost:{}", task.id, System.currentTimeMillis() - start);
        return error;
    }
}
