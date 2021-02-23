// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.adc.service.sigmob;

import com.adtiming.om.adc.dto.ReportAdnData;
import com.adtiming.om.adc.dto.ReportTask;
import com.adtiming.om.adc.service.AdnBaseService;
import com.adtiming.om.adc.service.AppConfig;
import com.adtiming.om.adc.util.MapHelper;
import com.adtiming.om.adc.util.MyHttpClient;
import com.adtiming.om.adc.util.ParamsBuilder;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.codec.digest.DigestUtils;
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
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class DownloadSigmob extends AdnBaseService {

    private static final Logger LOG = LogManager.getLogger();

    private static final String COUNTRY_CN = "CN";

    @Resource
    private JdbcTemplate jdbcTemplateW;

    @Resource
    private AppConfig cfg;

    @Override
    public void setAdnInfo() {
        this.adnId = 20;
        this.adnName = "Sigmob";
        this.maxTaskCount = 10; //请求频率：每秒最多10次
    }

    @Override
    public void executeTaskImpl(ReportTask o) {
        try {
            executeSigmobTask(o);
        } catch (Exception e) {
            LOG.error("[{}] executeTaskImpl error, taskId:{}", this.adnName, o.id, e);
        }
    }

    private void executeSigmobTask(ReportTask task) {
        String publicKey = task.userId;
        String secret = task.userSignature;
        String day = task.day;

        LOG.info("[Sigmob] executeTaskImpl start, taskId:{}", task.id);
        long start = System.currentTimeMillis();
        updateTaskStatus(jdbcTemplateW, task.id, 1, "");
        StringBuilder err = new StringBuilder();
        String error;
        task.step = 1;
        String jsonData = downloadData(task, publicKey, secret, day, day, err);
        if (StringUtils.isNotBlank(jsonData) && err.length() == 0) {
            task.step = 2;
            error = jsonDataImportDatabase(task, jsonData, day);
            if (StringUtils.isBlank(error)) {
                task.step = 3;
                error = savePrepareReportData(task, day, publicKey);
                if (StringUtils.isBlank(error)) {
                    task.step = 4;
                    error = reportLinkedToStat(task, publicKey);
                }
            }
        } else {
            error = err.toString();
        }
        int status = getStatus(error);
        error = convertMsg(error);
        if (task.runCount >= 4 && status != 2) {
            updateAccountException(jdbcTemplateW, task, error);
            LOG.warn("[Sigmob] executeTaskImpl error,run count:{},taskId:{},msg:{}", task.runCount + 1, task.id, error);
        } else {
            updateTaskStatus(jdbcTemplateW, task.id, status, error);
        }
        LOG.info("[Sigmob] executeTaskImpl end, taskId:{}, cost:{}", task.id, System.currentTimeMillis() - start);
    }

    private String downloadData(ReportTask task, String publicKey, String secret, String startDate, String endDate, StringBuilder err) {
        String json_data = "";
        HttpEntity entity = null;
        LOG.info("[Sigmob] downloadData start, taskId:{}", task.id);
        long start = System.currentTimeMillis();
        try {
            String dimensions = "date,platform,application,placement,adType";
            String params = buildSigmobParams(publicKey, secret, dimensions, startDate, endDate);
            String url = String.format("https://report.sigmob.cn/pub/v1/apps/reports?%s", params);
            LOG.info("[Sigmob] request url:{}", url);
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
            json_data = EntityUtils.toString(entity);
            if (StringUtils.isNoneBlank(json_data) && json_data.contains("{\"msg\":")) {
                err.append(json_data);
                json_data = "";
            }
            return json_data;
        } catch (Exception e) {
            err.append(String.format("downJsonData error, msg:%s", e.getMessage()));
        } finally {
            EntityUtils.consumeQuietly(entity);
        }
        LOG.info("[Sigmob] downloadPageJsonData end, taskId:{}, cost:{}", task.id, System.currentTimeMillis() - start);
        return json_data;
    }

    private String buildSigmobParams(String publicKey, String secret, String dimensions, String start,String end) {
        ParamsBuilder pb = new ParamsBuilder()
                .p("pk", publicKey)
                .p("t", System.currentTimeMillis())
                .p("startDate", start)
                .p("endDate", end)
                .p("dimensions", dimensions);
        pb.params().sort(Comparator.comparing(NameValuePair::getName));
        List<String> params = pb.params().stream().map(NameValuePair::getValue).collect(Collectors.toList());
        String str = StringUtils.join(params, "") + secret;
        String sign = DigestUtils.sha1Hex(str.getBytes(StandardCharsets.UTF_8));
        pb.p("sign", sign);
        return pb.format();
    }

    private String jsonDataImportDatabase(ReportTask task, String jsonData, String day) {
        String sql_delete = "delete from report_sigmob where date=? and report_account_id=? ";
        try {
            jdbcTemplateW.update(sql_delete, day, task.reportAccountId);
        } catch (Exception e) {
            return String.format("delete report_Sigmob error,msg:%s", e.getMessage());
        }
        LOG.info("[Sigmob] jsonDataImportDatabase taskId:{}", task.id);
        long start = System.currentTimeMillis();
        String error = "";

        try {
            String sql_insert = "INSERT into report_sigmob (date, country, platform, app_id, app_name, placement_id, placement_name, ad_type, " +
                    "impressions, clicks, revenue, report_account_id) " +
                    " VALUES(?,?,?,?,?,?,?,?,?,?,?,?)";

            List<Object[]> lsParm = new ArrayList<>(1000);
            JSONArray jsonArray = JSONArray.parseArray(jsonData);
            for (int i = 0; i < jsonArray.size(); i++) {
                JSONObject obj = jsonArray.getJSONObject(i);
                Object[] params = new Object[]{day, COUNTRY_CN, obj.get("platform"),
                        obj.get("appId"), obj.get("appName"),
                        obj.get("placementId"), obj.get("placementName"), obj.get("adType"),
                        obj.get("impressions"), obj.get("clicks"),
                        obj.get("revenue"), task.reportAccountId};
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
            error = String.format("insert report_Sigmob error, msg:%s", e.getMessage());
        }
        LOG.info("[Sigmob] jsonDataImportDatabase end, taskId:{}, cost:{}", task.id, System.currentTimeMillis() - start);
        return error;
    }

    private String savePrepareReportData(ReportTask task, String reportDay, String publicKey) {
        LOG.info("[Sigmob] savePrepareReportData start, taskId:{}", task.id);
        long start = System.currentTimeMillis();
        String error;
        try {
            String dataSql = "select date day,country,concat(app_id,'-',placement_id) data_key,0 AS api_request,0 AS api_filled," +
                    "sum(impressions) AS api_impr,sum(clicks) AS api_click,sum(revenue) AS revenue " +
                    " from report_Sigmob where date=? and report_account_id=?" +
                    " group by date,country,app_id,placement_id";

            List<ReportAdnData> data = jdbcTemplateW.query(dataSql, ReportAdnData.ROWMAPPER, reportDay, task.reportAccountId);

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

            error = toAdnetworkLinked(task, publicKey, placements, data);
        } catch (Exception e) {
            error = String.format("savePrepareReportData error, msg:%s", e.getMessage());
        }
        LOG.info("[Sigmob] savePrepareReportData end, taskId:{}, cost:{}", task.id, System.currentTimeMillis() - start);
        return error;
    }
}
