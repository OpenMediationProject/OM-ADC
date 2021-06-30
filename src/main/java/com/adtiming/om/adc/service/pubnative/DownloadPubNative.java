package com.adtiming.om.adc.service.pubnative;

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
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
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

@Service
public class DownloadPubNative extends AdnBaseService {

    private static final Logger LOG = LogManager.getLogger();

    private static final DateTimeFormatter DATEFORMAT_DMY = DateTimeFormatter.ofPattern("dd/MM/yyyy");

    @Resource
    private JdbcTemplate jdbcTemplateW;

    @Resource
    private AppConfig cfg;

    @Override
    public void setAdnInfo() {
        this.adnId = 23;
        this.adnName = "PubNative";
    }

    @Override
    public void executeTaskImpl(ReportTask o) {
        try {
            executePubNativeTask(o);
        } catch (Exception e) {
            LOG.error("[{}] executeTaskImpl error, taskId:{}", this.adnName, o.id, e);
        }
    }

    private void executePubNativeTask(ReportTask task) {
        String apiKey = task.getAdnApiKey();
        String day = task.day;

        LOG.info("[PubNative] executeTaskImpl start, taskId:{}", task.id);
        long start = System.currentTimeMillis();
        updateTaskStatus(jdbcTemplateW, task.id, 1, "");
        StringBuilder err = new StringBuilder();
        String error;
        task.step = 1;
        String jsonData = downloadData(task, apiKey, day, err);
        if (StringUtils.isNoneBlank(jsonData) && err.length() == 0) {
            task.step = 2;
            error = jsonDataToDB(task, jsonData, day, apiKey);
            if (StringUtils.isBlank(error)) {
                task.step = 3;
                error = savePrepareReportData(task, day, apiKey);
                if (StringUtils.isBlank(error)) {
                    task.step = 4;
                    error = reportLinkedToStat(task, apiKey);
                }
            }
        } else {
            error = err.toString();
        }
        int status = getStatus(error);
        error = convertMsg(error);
        if (task.getRunCount() >= 4 && status != 2) {
            updateAccountException(jdbcTemplateW, task, error);
            LOG.warn("[PubNative] executeTaskImpl error,run count:{},taskId:{},msg:{}", task.getRunCount() + 1, task.id, error);
        } else {
            updateTaskStatus(jdbcTemplateW, task.id, status, error);
        }
        LOG.info("[PubNative] executeTaskImpl end, reportApiKey:{}, day:{}, cost:{}", apiKey, day, System.currentTimeMillis() - start);
    }

    private String downloadData(ReportTask task, String apiKey, String day, StringBuilder err) {
        String json_data = "";
        HttpEntity entity = null;
        LOG.info("[PubNative] downloadData start, taskId:{}", task.id);
        long start = System.currentTimeMillis();
        try {
            String date = LocalDate.parse(day, DATEFORMAT_YMD).format(DATEFORMAT_DMY);
            String groupBy = "group_by[]=zone_id&group_by[]=country_code&group_by[]=store_app_id";
            String url = String.format("https://dashboard.pubnative.net/api/reports?account_auth_token=%s&start_date=%s&end_date=%s&%s",
                    apiKey, date, date, groupBy);
            LOG.info("[PubNative] request url:{}", url);
            updateReqUrl(jdbcTemplateW, task.id, url);
            HttpGet httpGet = new HttpGet(url);
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
            String data = EntityUtils.toString(entity);
            if (StringUtils.isNoneBlank(data)) {
                JSONObject result = JSONObject.parseObject(data);
                String error = result.getString("errors");
                String message = result.getString("message");
                if (StringUtils.isBlank(error) && StringUtils.isBlank(message)) {
                    json_data = result.getString("reports");
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
        LOG.info("[PubNative] downloadData end, taskId:{}, cost:{}", task.id, System.currentTimeMillis() - start);
        return json_data;
    }

    private String jsonDataToDB(ReportTask task, String jsonData, String day, String apiKey) {
        String sql_delete = "delete from report_pubnative where day=? and report_api_key=? ";
        try {
            jdbcTemplateW.update(sql_delete, day, apiKey);
        } catch (Exception e) {
            return String.format("delete report_pubnative error,msg:%s", e.getMessage());
        }
        LOG.info("[PubNative] jsonDataImportDatabase start, taskId:{}", task.id);
        long start = System.currentTimeMillis();
        String error = "";
        int count = 0;
        try {
            String sql_insert = "INSERT into report_pubnative (day, date, country, store_app_id, app_name, zone_id, ad_server_requests, ad_server_filled_requests, impressions, clicks, revenues, report_api_key) " +
                    " VALUES(?,?,?,?,?,?,?,?,?,?,?,?)";

            List<Object[]> lsParm = new ArrayList<>(1000);
            JSONArray jsonArray = JSONArray.parseArray(jsonData);
            for (int i = 0; i < jsonArray.size(); i++) {
                JSONObject obj = jsonArray.getJSONObject(i);
                count++;
                Object[] params = new Object[]{task.day, obj.get("date"), obj.get("country_code"),
                        StringUtils.trim(MapHelper.getString(obj, "store_app_id")), obj.get("app_name"), obj.get("zone_id"), obj.get("ad_server_requests"),
                        obj.get("ad_server_filled_requests"), obj.get("impressions"),
                        obj.get("clicks"), obj.get("revenues"), apiKey};
                if (lsParm.size() > 1000) {
                    jdbcTemplateW.batchUpdate(sql_insert, lsParm);
                    lsParm = new ArrayList<>();
                }
                lsParm.add(params);
            }
            if (!lsParm.isEmpty()) {
                jdbcTemplateW.batchUpdate(sql_insert, lsParm);
            }
        } catch (Exception e) {
            error = String.format("insert report_pubnative error, msg:%s", e.getMessage());
        }
        LOG.info("[PubNative] jsonDataImportDatabase end, taskId:{}, insert count:{}, cost:{}", task.id, count, System.currentTimeMillis() - start);
        return error;
    }

    private String savePrepareReportData(ReportTask task, String reportDay, String appKey) {
        LOG.info("[PubNative] savePrepareReportData start, taskId:{}", task.id);
        long start = System.currentTimeMillis();
        String error;
        try {
            String dataSql = "select day,country,concat(trim(store_app_id),'-',zone_id) data_key,sum(ad_server_requests) AS api_request,sum(ad_server_filled_requests) AS api_filled," +
                    "sum(impressions) AS api_impr,sum(clicks) AS api_click,sum(revenues) AS revenue " +
                    " from report_pubnative where day=? and report_api_key=?" +
                    " group by day,country,store_app_id,zone_id ";

            List<ReportAdnData> data = jdbcTemplateW.query(dataSql, ReportAdnData.ROWMAPPER, reportDay, appKey);

            if (data.isEmpty())
                return "data is null";
            List<Map<String, Object>> instanceInfoList = getInstanceList(task.getReportAccountId());
            if (instanceInfoList.isEmpty()) {
                return "instance is null";
            }
            LocalDate dataDay = LocalDate.parse(task.day, DATEFORMAT_YMD);
            Map<String, Map<String, Object>> placements = new HashMap<>();
            for (Map<String, Object> ins : instanceInfoList) {
                String key = MapHelper.getString(ins, "app_id") + "-"
                        + MapHelper.getString(ins, "placement_key");
                putLinkKeyMap(placements, key, ins, dataDay);
            }

            error = toAdnetworkLinked(task, appKey, placements, data);
        } catch (Exception e) {
            error = String.format("savePrepareReportData error, msg:%s", e.getMessage());
        }
        LOG.info("[PubNative] savePrepareReportData end, skey:{}, day:{}, cost:{}", appKey, reportDay, System.currentTimeMillis() - start);
        return error;
    }
}
