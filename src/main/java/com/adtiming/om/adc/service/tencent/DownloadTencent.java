// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.adc.service.tencent;

import com.adtiming.om.adc.dto.ReportAdnData;
import com.adtiming.om.adc.dto.ReportTask;
import com.adtiming.om.adc.service.AdnBaseService;
import com.adtiming.om.adc.service.AppConfig;
import com.adtiming.om.adc.util.MapHelper;
import com.adtiming.om.adc.util.MyHttpClient;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.codec.binary.Base64;
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
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class DownloadTencent extends AdnBaseService {
    private static final Logger LOG = LogManager.getLogger();

    @Resource
    private JdbcTemplate jdbcTemplate;

    @Resource
    private AppConfig cfg;

    @Override
    public void setAdnInfo() {
        this.adnId = 6;
        this.adnName = "Tencent";
    }

    @Override
    public void executeTaskImpl(ReportTask o) {
        try {
            executeTencentTask(o);
        } catch (Exception e) {
            LOG.error("[{}] executeTaskImpl error, taskId:{}", this.adnName, o.id, e);
        }
    }

    private void executeTencentTask(ReportTask task) {
        String userId = task.userId;
        String userSign = task.userSignature;
        String day = task.day;
        // Set up AdSense Management API client.
        if (StringUtils.isBlank(userId) || StringUtils.isBlank(userSign)) {
            LOG.error("[Tencent] executeTencentTask error, user_id or user_signature is null");
            return;
        }

        LOG.info("[Tencent] executeTencentTask start, userId:{}, userSign:{}, day:{}", userId, userSign, day);
        long start = System.currentTimeMillis();
        updateTaskStatus(jdbcTemplate, task.id, 1, "");
        StringBuilder err = new StringBuilder();
        String error;
        task.step = 1;
        String json_data = downJsonData(task.id, userId, userSign, day, err);
        if (StringUtils.isNotBlank(json_data) && err.length() == 0) {
            task.step = 2;
            error = jsonDataImportDatabase(json_data, day, userId, userSign);
            if (StringUtils.isBlank(error)) {
                task.step = 3;
                error = savePrepareReportData(task, day, userId);
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
            updateAccountException(jdbcTemplate, task, error);
            LOG.warn("[Tencent] executeTaskImpl error,run count:{},taskId:{},msg:{}", task.runCount + 1, task.id, error);
        } else {
            updateTaskStatus(jdbcTemplate, task.id, status, error);
        }
        LOG.info("[Tencent] executeTencentTask end, userId:{}, userSign:{}, day:{}, cost:{}", userId, userSign, day, System.currentTimeMillis() - start);
    }

    private String downJsonData(int taskId, String userId, String userSign, String day, StringBuilder err) {
        String json_data = "";
        HttpEntity entity = null;
        LOG.info("[Tencent] downJsonData start, taskId:{}, userId:{}, userSign:{}, day:{}", taskId, userId, userSign, day);
        long start = System.currentTimeMillis();
        try {
            long time = System.currentTimeMillis() / 1000;
            // sha1(memberid + secret + time)
            // base64(memberid + ',' + time + ',' + sign)
            String sign = DigestUtils.sha1Hex(String.format("%s%s%d", userId, userSign, time));
            String token = Base64.encodeBase64String(String.format("%s,%d,%s", userId, time, sign).getBytes(StandardCharsets.UTF_8));
            String startDate = LocalDate.parse(day, DATEFORMAT_YMD).format(DATEFORMAT_DAY);
            //String url = String.format("https://test-api.adnet.qq.com/open/v1.1/report/get?member_id=%s&start_date=%s&end_date=%s", userId, startDate, startDate);
            String url = String.format("https://api.adnet.qq.com/open/v1.1/report/get?member_id=%s&start_date=%s&end_date=%s", userId, startDate, startDate);
            LOG.info("[Tencent] request url:{}, token:{}", url, token);
            updateReqUrl(jdbcTemplate, taskId, url + "&token=" + token);
            HttpGet httpGet = new HttpGet(url);
            httpGet.setHeader("token", token);
            RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(5 * 60 * 1000).setProxy(cfg.httpProxy).build();//设置请求和传输超时时间
            httpGet.setConfig(requestConfig);
            //发送Post,并返回一个HttpResponse对象
            HttpResponse response = MyHttpClient.getInstance().execute(httpGet);
            StatusLine sl = response.getStatusLine();
            entity = response.getEntity();
            if (sl.getStatusCode() != 200) {//如果状态码为200,就是正常返回
                err.append(String.format("request report response statusCode:%d,msg:%s", sl.getStatusCode(), entity == null ? "" : EntityUtils.toString(entity)));
                return json_data;
            }
            if (entity == null) {
                err.append("request report response enity is null");
                return json_data;
            }
            String respData = EntityUtils.toString(entity);
            JSONObject jobj = JSONObject.parseObject(respData);
            int code = jobj.getInteger("code");
            if (code == 0) {
                JSONObject list = jobj.getJSONObject("data");
                if (list != null) {
                    String strData = list.getString("list");
                    if (StringUtils.isBlank(strData) || "null".equals(strData) || "[]".equals(strData)) {
                        err.append("data is null");
                        return "";
                    }
                    json_data = strData;
                } else {
                    err.append("data is null");
                    return "";
                }
            } else {
                err.append(respData);
            }
        } catch (Exception ex) {
            err.append(ex.getMessage());
        } finally {
            EntityUtils.consumeQuietly(entity);
        }
        LOG.info("[Tencent] downJsonData end, taskId:{}, userId:{}, userSign:{}, day:{}, cost:{}", taskId, userId, userSign, day, System.currentTimeMillis() - start);
        return json_data;
    }

    private String jsonDataImportDatabase(String jsonData, String day, String userId, String userSign) {
        String sql_delete = "DELETE FROM report_tencent WHERE day=? AND member_id=?";
        try {
            jdbcTemplate.update(sql_delete, day, userId);
        } catch (Exception e) {
            return String.format("delete report_tencent error,msg:%s", e.getMessage());
        }
        LOG.info("[Tencent] jsonDataImportDatabase start, userId:{}, userSign:{}, day:{}", userId, userSign, day);
        long start = System.currentTimeMillis();
        String error = "";

        String sql_insert = "INSERT INTO report_tencent (day, member_id, medium_name, app_id,placement_id,placement_name,placement_type,is_summary,request_count,return_count, ad_request_count, ad_return_count, pv, click, fill_rate,ad_exposure_rate,click_rate,revenue,ecpm) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        try {
            int count = 0;
            List<Object[]> lsParm = new ArrayList<>();
            JSONArray jsonArray = JSONArray.parseArray(jsonData);
            for (int i = 0; i < jsonArray.size(); i++) {
                JSONObject obj = jsonArray.getJSONObject(i);
                count++;
                Object[] params = new Object[]{ obj.get("date"), obj.get("member_id"), obj.get("medium_name"),
                        obj.get("app_id"), obj.get("placement_id"), obj.getString("placement_name"),
                        obj.getString("placement_type"), obj.getBoolean("is_summary") ? 1 : 0,
                        obj.getIntValue("request_count"), obj.getIntValue("return_count"),
                        obj.getIntValue("ad_request_count"), obj.getIntValue("ad_return_count"),
                        obj.getIntValue("pv"), obj.getIntValue("click"),
                        obj.get("fill_rate"), obj.get("ad_exposure_rate"),
                        obj.get("click_rate"), obj.getDoubleValue("revenue"), obj.getDoubleValue("ecpm")};
                if (count > 1000) {
                    jdbcTemplate.batchUpdate(sql_insert, lsParm);
                    count = 1;
                    lsParm = new ArrayList<>();
                }
                lsParm.add(params);
            }
            if (!lsParm.isEmpty()) {
                jdbcTemplate.batchUpdate(sql_insert, lsParm);
            }
        } catch (Exception e) {
            error = String.format("insert report_tencent error, msg:%s", e.getMessage());
        }
        LOG.info("[Tencent] jsonDataImportDatabase end, userId:{}, userSign:{}, day:{}, cost:{}", userId, userSign, day, System.currentTimeMillis() - start);
        return error;
    }

    private String savePrepareReportData(ReportTask task, String reportDay, String secureKey) {
        LOG.info("[Tencent] savePrepareReportData start, taskId:{}", task.id);
        long start = System.currentTimeMillis();
        String error;
        try {
            List<Map<String, Object>> instanceInfoList = getInstanceList(task.reportAccountId);

            if (instanceInfoList.isEmpty()) {
                return "instance is null";
            }
            LocalDate dataDay = LocalDate.parse(task.day, DATEFORMAT_YMD);
            Map<String, Map<String, Object>> placements = new HashMap<>();
            for (Map<String, Object> ins : instanceInfoList) {
                String key = MapHelper.getString(ins, "app_key") + "-" + MapHelper.getString(ins, "placement_key");
                putLinkKeyMap(placements, key, ins, dataDay);
            }

            String dataSql = "select day,'CN' country,concat(app_id,'-',placement_id) data_key," +
                    "sum(request_count) api_request,sum(return_count) api_filled," +
                    "sum(pv) api_impr,sum(click) api_click,sum(revenue) AS revenue " +
                    " from report_tencent where day=? and member_id=? group by day,app_id,placement_id ";

            List<ReportAdnData> oriDataList = jdbcTemplate.query(dataSql, ReportAdnData.ROWMAPPER, reportDay, secureKey);

            if (oriDataList.isEmpty())
                return "data is null";
            error = toAdnetworkLinked(task, secureKey, placements, oriDataList);
        } catch (Exception e) {
            error = String.format("savePrepareReportData error, msg:%s", e.getMessage());
        }
        LOG.info("[Tencent] savePrepareReportData end, taskId:{}, cost:{}", task.id, System.currentTimeMillis() - start);
        return error;
    }
}
