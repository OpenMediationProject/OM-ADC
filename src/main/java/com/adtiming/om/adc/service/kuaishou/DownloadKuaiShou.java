// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.adc.service.kuaishou;

import com.adtiming.om.adc.dto.ReportAdnData;
import com.adtiming.om.adc.dto.ReportTask;
import com.adtiming.om.adc.service.AdnBaseService;
import com.adtiming.om.adc.util.MapHelper;
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
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class DownloadKuaiShou extends AdnBaseService {

    private static final Logger LOG = LogManager.getLogger();

    @Resource
    private JdbcTemplate jdbcTemplateW;

    @Override
    public void setAdnInfo() {
        this.adnId = 21;
        this.adnName = "KuaiShou";
    }

    @Override
    public void executeTaskImpl(ReportTask o) {
        try {
            executeKuaiShouTask(o);
        } catch (Exception e) {
            LOG.error("[{}] executeTaskImpl error, taskId:{}", this.adnName, o.id, e);
        }
    }

    private void executeKuaiShouTask(ReportTask task) {
        String accessKey = task.userId;
        String security = task.userSignature;
        String day = task.day;
        // Set up AdSense Management API client.
        if (StringUtils.isBlank(accessKey)) {
            LOG.error("[KuaiShou] skey is null, taskId:{}", task.id);
            return;
        }
        if (StringUtils.isBlank(security)) {
            LOG.error("[KuaiShou] secret is null, taskId:{}", task.id);
            return;
        }

        LOG.info("[KuaiShou] executeTaskImpl start, taskId:{}", task.id);
        long start = System.currentTimeMillis();
        updateTaskStatus(jdbcTemplateW, task.id, 1, "");
        StringBuilder err = new StringBuilder();
        String error;
        task.step = 1;
        String jsonData = downloadData(task, accessKey, security, day, err);
        if (StringUtils.isNoneBlank(jsonData) && err.length() == 0) {
            task.step = 2;
            error = jsonDataToDB(task, jsonData, day, accessKey);
            if (StringUtils.isBlank(error)) {
                task.step = 3;
                error = savePrepareReportData(task, day, accessKey);
                if (StringUtils.isBlank(error)) {
                    task.step = 4;
                    error = reportLinkedToStat(task, accessKey);
                }
            }
        } else {
            error = err.toString();
        }
        int status = getStatus(error);
        error = convertMsg(error);
        if (task.runCount >= 4 && status != 2) {
            updateAccountException(jdbcTemplateW, task, error);
            LOG.warn("[KuaiShou] executeTaskImpl error,run count:{},taskId:{},msg:{}", task.runCount + 1, task.id, error);
        } else {
            updateTaskStatus(jdbcTemplateW, task.id, status, error);
        }
        LOG.info("[KuaiShou] executeTaskImpl end, skey:{}, secret:{}, day:{}, cost:{}", accessKey, security, day, System.currentTimeMillis() - start);
    }

    private String downloadData(ReportTask task, String accessKey, String securityKey, String day, StringBuilder err) {
        String json_data = "";
        HttpEntity entity = null;
        LOG.info("[KuaiShou] downloadData start, taskId:{}", task.id);
        long start = System.currentTimeMillis();
        try {
            long timeStamp = System.currentTimeMillis() / 1000; // 时间戳到秒
            String sign = DigestUtils.md5Hex(String.format("/api/report/dailyShare?ak=%s&date=%s&sk=%s&timestamp=%d", accessKey, day, securityKey, timeStamp));
            String url = String.format("https://ssp.e.kuaishou.com/api/report/dailyShare?date=%s&timestamp=%d&ak=%s&sign=%s", day, timeStamp, accessKey, sign);
            LOG.info("[KuaiShou] request url:{}", url);
            updateReqUrl(jdbcTemplateW, task.id, url);
            HttpGet httpGet = new HttpGet(url);
            RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(5 * 60 * 1000)
                    //.setProxy(cfg.httpProxy)
                    .build();//设置请求和传输超时时间
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
            String data = EntityUtils.toString(entity);
            if (StringUtils.isNoneBlank(data)) {
                JSONObject result = JSONObject.parseObject(data);
                int code = result.getIntValue("result");
                if (code == 1) {
                    json_data = result.getString("data");
                } else {
                    err.append(data);
                    json_data = "";
                }
            }
            return json_data;
        } catch (Exception e) {
            err.append(String.format("downJsonData error, msg:%s", e.getMessage()));
        } finally {
            EntityUtils.consumeQuietly(entity);
        }
        LOG.info("[KuaiShou] downloadData end, taskId:{}, cost:{}", task.id, System.currentTimeMillis() - start);
        return json_data;
    }

    private String jsonDataToDB(ReportTask task, String jsonData, String day, String appKey) {
        String sql_delete = "delete from report_kuaishou where day=? and access_key=? ";
        try {
            jdbcTemplateW.update(sql_delete, day, appKey);
        } catch (Exception e) {
            return String.format("delete report_kuaishou error,msg:%s", e.getMessage());
        }
        LOG.info("[KuaiShou] jsonDataImportDatabase start, taskId:{}", task.id);
        long start = System.currentTimeMillis();
        String error = "";

        try {
            String sql_insert = "INSERT into report_kuaishou (day, name, app_id, position_id, req_cnt, resp_cnt, impression, click, share, ecpm, access_key) " +
                    " VALUES(?,?,?,?,?,?,?,?,?,?,?)";

            List<Object[]> lsParm = new ArrayList<>(1000);
            JSONArray jsonArray = JSONArray.parseArray(jsonData);
            for (int i = 0; i < jsonArray.size(); i++) {
                JSONObject obj = jsonArray.getJSONObject(i);
                Object[] params = new Object[]{day, obj.get("name"), obj.get("app_id"), obj.get("position_id"),
                        obj.getIntValue("req_cnt"), obj.getIntValue("resp_cnt"),
                        obj.getIntValue("impression"), obj.getIntValue("click"),
                        obj.get("share") == null ? 0 : obj.get("share"),
                        obj.get("ecpm") == null ? 0 : obj.get("ecpm"), appKey};
                lsParm.add(params);
                if (lsParm.size() == 1000) {
                    jdbcTemplateW.batchUpdate(sql_insert, lsParm);
                    lsParm.clear();
                }
            }
            if (!lsParm.isEmpty()) {
                jdbcTemplateW.batchUpdate(sql_insert, lsParm);
            }
        } catch (Exception e) {
            error = String.format("insert report_kuaishou error, msg:%s", e.getMessage());
        }
        LOG.info("[KuaiShou] jsonDataImportDatabase end, taskId:{}, cost:{}", task.id, System.currentTimeMillis() - start);
        return error;
    }

    private String savePrepareReportData(ReportTask task, String reportDay, String appKey) {
        LOG.info("[KuaiShou] savePrepareReportData start, taskId:{}", task.id);
        long start = System.currentTimeMillis();
        String error;
        try {
            String dataSql = "select day,'CN' country,concat(app_id,'-',position_id) data_key,sum(req_cnt) AS api_request,sum(resp_cnt) AS api_filled," +
                    "sum(impression) AS api_impr,sum(click) AS api_click,sum(share) AS revenue " +
                    " from report_kuaishou where day=? and access_key=?" +
                    " group by day,app_id,position_id ";

            List<ReportAdnData> data = jdbcTemplateW.query(dataSql, ReportAdnData.ROWMAPPER, reportDay, appKey);

            if (data.isEmpty())
                return "data is null";

            List<Map<String, Object>> instanceInfoList = getInstanceList(task.reportAccountId);
            if (instanceInfoList.isEmpty()) {
                return "instance is null";
            }
            LocalDate dataDay = LocalDate.parse(task.day, DATEFORMAT_YMD);
            Map<String, Map<String, Object>> placements = new HashMap<>();
            for (Map<String, Object> ins : instanceInfoList) {
                String key = MapHelper.getString(ins, "app_key") + "-"
                        + MapHelper.getString(ins, "placement_key");
                putLinkKeyMap(placements, key, ins, dataDay);
            }

            error = toAdnetworkLinked(task, appKey, placements, data);
        } catch (Exception e) {
            error = String.format("savePrepareReportData error, msg:%s", e.getMessage());
        }
        LOG.info("[KuaiShou] savePrepareReportData end, skey:{}, day:{}, cost:{}", appKey, reportDay, System.currentTimeMillis() - start);
        return error;
    }
}
