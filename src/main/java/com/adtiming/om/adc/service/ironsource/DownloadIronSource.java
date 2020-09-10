// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3
package com.adtiming.om.adc.service.ironsource;

import com.adtiming.om.adc.dto.ReportAdnData;
import com.adtiming.om.adc.dto.ReportTask;
import com.adtiming.om.adc.service.AdnBaseService;
import com.adtiming.om.adc.service.AppConfig;
import com.adtiming.om.adc.util.MapHelper;
import com.adtiming.om.adc.util.MyHttpClient;
import com.adtiming.om.adc.util.Util;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.codec.binary.Base64;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;

@Service
public class DownloadIronSource extends AdnBaseService {

    private static final Logger LOG = LogManager.getLogger();

    private static final String REPORT_URL = "https://platform.ironsrc.com/partners/publisher/mediation/applications/v5/stats?startDate=%s&endDate=%s&breakdowns=date,app,platform,adSource,adUnits,instance,country";

    @Resource
    private AppConfig cfg;

    @Resource
    private JdbcTemplate jdbcTemplate;

    @Override
    public void setAdnInfo() {
        this.adnId = 15;
        this.adnName = "ironSource";
        this.maxTaskCount = 10;//IronSource is limited to 20 requests every 10 minutes.
    }

    @Override
    public void executeTaskImpl(ReportTask o) {
        try {
            downloadData(o);
        } catch (Exception e) {
            LOG.error("[{}] executeTaskImpl error, taskId:{}", this.adnName, o.id);
        }
    }
    private void downloadData(ReportTask task) {
        //String appKey = task.adn_app_id;
        String username = task.userId;
        String secretKey = task.userSignature;
        String day = task.day;
        if (StringUtils.isBlank(username) || StringUtils.isBlank(secretKey)) {
            LOG.error("[IronSource]，appKey is null");
            return;
        }
        LOG.info("[IronSource] executeTaskImpl start, taskId:{}", task.id);
        long start = System.currentTimeMillis();
        updateTaskStatus(jdbcTemplate, task.id, 1, "");
        StringBuilder err = new StringBuilder();
        String error;
        task.step = 1;
        String json_data = downJsonData(task.id, username, secretKey, day, err);
        if (StringUtils.isNotBlank(json_data) && err.length() == 0) {
            task.step = 2;
            error = jsonDataImportDB(json_data, day, username);
            if (StringUtils.isBlank(error)) {
                task.step = 3;
                error = savePrepareReportData(task, username);
                if (StringUtils.isBlank(error)) {
                    task.step = 4;
                    error = reportLinkedToStat(task, username);
                }
            }
        } else {
            error = err.toString();
        }
        int status = getStatus(error);
        error = convertMsg(error);
        if (task.runCount >= 4 && status != 2) {
            updateAccountException(jdbcTemplate, task, error);
            LOG.error("[IronSource] executeTaskImpl error,run count:{},taskId:{},msg:{}", task.runCount + 1, task.id, error);
        } else {
            updateTaskStatus(jdbcTemplate, task.id, status, error);
        }
        LOG.info("[IronSource] executeTaskImpl end, taskId:{}, cost:{}", task.id,System.currentTimeMillis() - start);
    }

    private String downJsonData(int taskId, String username, String secretKey, String day, StringBuilder err) {
        String json_data = "";
        HttpEntity entity = null;
        LOG.info("[IcronSource] downJsonData start, taskId:{}", taskId);
        long start = System.currentTimeMillis();
        try {
            String token = Base64.encodeBase64String((username + ":" + secretKey).getBytes(UTF_8));
            String reportUrl = String.format(REPORT_URL, day, day);
            updateReqUrl(jdbcTemplate, taskId, reportUrl);
            HttpGet httpGet = new HttpGet(reportUrl);
            httpGet.setHeader("Authorization", "Basic " + token);
            httpGet.setHeader("Accept", "application/json; */*");
            RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(5 * 60 * 1000).setProxy(cfg.httpProxy).build();//设置请求和传输超时时间
            httpGet.setConfig(requestConfig);
            //发送Post,并返回一个HttpResponse对象
            HttpResponse response = MyHttpClient.getInstance().execute(httpGet);
            StatusLine sl = response.getStatusLine();
            int statusCode = sl.getStatusCode();
            if (statusCode == 204) {
                err.append("data is null");
                return "";
            }
            if (sl.getStatusCode() != 200) {//如果状态码为200,就是正常返回
                err.append(String.format("request report response statusCode:%d,msg:%s", sl.getStatusCode(), EntityUtils.toString(response.getEntity())));
                return json_data;
            }
            entity = response.getEntity();
            if (entity == null) {
                err.append("request report response enity is null");
                return json_data;
            }
            json_data = EntityUtils.toString(entity);
        } catch (Exception ex) {
            err.append(String.format("downJsonData error:%s", ex.getMessage()));
        } finally {
            EntityUtils.consumeQuietly(entity);
        }
        LOG.info("[IcronSource] downJsonData end, taskId:{}, cost:{}", taskId, System.currentTimeMillis() - start);
        return json_data;
    }

    private String jsonDataImportDB(String jsonData, String day, String username) {

        LOG.info("[IcronSource] jsonDataImportDatabase start, skey:{}, day:{}", username, day);
        long start = System.currentTimeMillis();
        String error = "";
        int dataCount = 0;
        try {
            String deleteSql = "DELETE FROM report_ironsource WHERE `date`=? AND username=? ";
            jdbcTemplate.update(deleteSql, day, username);

            String insertSql = "INSERT INTO report_ironsource (`date`, country_code, app_key, platform, ad_units, instance_id, instance_name, bundle_id, " +
                    "app_name, revenue, ecpm, impressions, active_users, engaged_users, engagement_rate, impressions_per_engaged_user, revenue_per_active_user, " +
                    "revenue_per_engaged_user,engaged_sessions,impression_per_engaged_sessions,impressions_per_session,sessions_per_active_user,ad_source_checks,ad_source_responses,ad_source_availability_rate,clicks,click_through_rate,username) " +
                    " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

            List<Object[]> lsParm = new ArrayList<>(1000);
            JSONArray jsonArray = JSONArray.parseArray(jsonData);
            for (int i = 0; i < jsonArray.size(); i++) {
                JSONObject obj = jsonArray.getJSONObject(i);
                JSONArray data = obj.getJSONArray("data");
                if (!data.isEmpty()) {
                    String date = Util.getJSONString(obj, "date");
                    String appKeyStr = Util.getJSONString(obj, "appKey");
                    String platform = Util.getJSONString(obj, "platform");
                    String adUnits = Util.getJSONString(obj, "adUnits");
                    int insId = Util.getJSONInt(obj, "instanceId");
                    String insName = Util.getJSONString(obj, "instanceName");
                    String bundleId = Util.getJSONString(obj, "bundleId");
                    String appName = Util.getJSONString(obj, "appName");
                    for (int j = 0; j < data.size(); j++) {
                        JSONObject countryData = data.getJSONObject(j);
                        String country = Util.getJSONString(countryData, "countryCode");
                        Object[] params = new Object[]{date, country,
                                appKeyStr, platform, adUnits, insId, insName, bundleId, appName,
                                Util.getJSONDecimal(countryData, "revenue"),
                                Util.getJSONDecimal(countryData, "eCPM"), Util.getJSONInt(countryData, "impressions"),
                                Util.getJSONInt(countryData, "activeUsers"), Util.getJSONInt(countryData, "engagedUsers"),
                                Util.getJSONDecimal(countryData, "engagementRate"), Util.getJSONDecimal(countryData, "impressionsPerEngagedUser"),
                                Util.getJSONDecimal(countryData, "revenuePerActiveUser"), Util.getJSONDecimal(countryData, "revenuePerEngagedUser"),
                                Util.getJSONInt(countryData, "engagedSessions"), Util.getJSONDecimal(countryData, "impressionPerEngagedSessions"),
                                Util.getJSONDecimal(countryData, "impressionsPerSession"), Util.getJSONDecimal(countryData, "sessionsPerActiveUser"),
                                Util.getJSONInt(countryData, "adSourceChecks"), Util.getJSONInt(countryData, "adSourceResponses"),
                                Util.getJSONDecimal(countryData, "adSourceAvailabilityRate"), Util.getJSONInt(countryData, "clicks"),
                                Util.getJSONDecimal(countryData, "clickThroughRate"), username
                        };
                        lsParm.add(params);
                        dataCount++;
                        if (lsParm.size() >= 1000) {
                            jdbcTemplate.batchUpdate(insertSql, lsParm);
                            lsParm.clear();
                        }
                    }
                }
            }
            if (!lsParm.isEmpty()) {
                jdbcTemplate.batchUpdate(insertSql, lsParm);
            }
        } catch (Exception e) {
            error = String.format("insert report_ironsource error, msg:%s", e.getMessage());
        }
        LOG.info("[IronSource] jsonDataImportDatabase end, skey:{}, day:{}, count:{}, cost:{}", username, day, dataCount, System.currentTimeMillis() - start);
        return error;
    }

    private String getAdTypeString(int adType) {
        String typeStr = "";
        switch (adType) {
            case 0:
                typeStr = "Banner";
                break;
            case 2:
                typeStr = "Rewarded Video";
                break;
            case 3:
                typeStr = "Interstitial";
                break;
        }
        return typeStr;
    }

    private String savePrepareReportData(ReportTask task, String username) {
        long start = System.currentTimeMillis();
        LOG.info("[IronSource] savePrepareReportData start,taskId:{}", task.id);
        String error;
        try {
            String dataSql = "select `date` day,country_code country,concat(app_key,'_',ad_units,'_',instance_id) data_key," +
                    "sum(ad_source_checks) api_request,sum(ad_source_responses) api_filled,sum(impressions) AS api_impr,sum(clicks) AS api_click,sum(revenue) AS revenue" +
                    " from report_ironsource " +
                    " where `date` =? and username=? " +
                    " group by `date`,country_code,ad_units,instance_id";

            List<ReportAdnData> oriDataList = jdbcTemplate.query(dataSql, ReportAdnData.ROWMAPPER, task.day, username);
            if (oriDataList.isEmpty())
                return "data is null";

            List<Map<String, Object>> instanceInfoList = getInstanceList(task.reportAccountId);
            Map<String, Map<String, Object>> placements = instanceInfoList.stream()
                    .collect(Collectors.toMap(m ->
                                    String.format("%s_%s_%s", MapHelper.getString(m, "adn_app_key"), getAdTypeString(MapHelper.getInt(m, "ad_type")),
                                            MapHelper.getString(m, "placement_key")),
                            m -> m, (existingValue, newValue) -> existingValue));

            // instance's placement_key changed
            Set<Integer> insIds = instanceInfoList.stream().map(o->MapHelper.getInt(o, "instance_id")).collect(Collectors.toSet());
            List<Map<String, Object>> oldInstanceList = getOldInstanceList(insIds);
            if (!oldInstanceList.isEmpty()) {
                placements.putAll(oldInstanceList.stream()
                        .collect(Collectors.toMap(m ->
                                        String.format("%s_%s_%s", MapHelper.getString(m, "adn_app_key"), getAdTypeString(MapHelper.getInt(m, "ad_type")),
                                                MapHelper.getString(m, "placement_key")),
                                m -> m, (existingValue, newValue) -> existingValue)));
            }
            error = toAdnetworkLinked(task, username, placements, oriDataList);
        } catch (Exception e) {
            error = String.format("savePrepareReportData error:%s", e.getMessage());
        }
        LOG.info("[IronSource] savePrepareReportData end, taskId:{}, cost:{}", task.id, System.currentTimeMillis() - start);
        return error;
    }
}
