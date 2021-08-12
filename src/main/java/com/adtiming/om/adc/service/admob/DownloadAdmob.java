// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.adc.service.admob;

import com.adtiming.om.adc.dto.ReportAdnData;
import com.adtiming.om.adc.dto.ReportTask;
import com.adtiming.om.adc.service.AdnBaseService;
import com.adtiming.om.adc.service.AppConfig;
import com.adtiming.om.adc.util.MapHelper;
import com.adtiming.om.adc.util.Util;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.util.Utils;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.JsonObjectParser;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.admob.v1.AdMob;
import com.google.api.services.admob.v1.model.Date;
import com.google.api.services.admob.v1.model.*;
import com.google.api.services.adsense.AdSense;
import com.google.api.services.adsense.model.AdsenseReportsGenerateResponse;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import javax.annotation.Resource;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

@Service
public class DownloadAdmob extends AdnBaseService {

    private static final Logger LOG = LogManager.getLogger();
    private static final JsonFactory JSON_FACTORY = Utils.getDefaultJsonFactory();

    @Resource
    private JdbcTemplate jdbcTemplate;

    @Resource
    private AppConfig cfg;

    @Override
    public void setAdnInfo() {
        this.adnId = 2;
        this.adnName = "Admob";
    }

    @Override
    public void executeTaskImpl(ReportTask o) {
        try {
            excuteTask(o);
        } catch (Exception e) {
            LOG.error("[{}] executeTaskImpl error, taskId:{}", this.adnName, o.id, e);
        }
    }

    private void excuteTask(ReportTask task) {
        LOG.info("[Admob] excuteTask start, taskId:{}", task.id);
        long start = System.currentTimeMillis();
        StringBuilder err = new StringBuilder();
        String error;
        try {
            updateTaskStatus(jdbcTemplate, task.id, 1, "");
            AdsenseReportsGenerateResponse res;
            task.step = 1;
            //final String accountKey = "ca-app-" + task.userId;
            if (task.authType == 3 || task.authType == 4) {//3:Use AdMobAPI
                List<GenerateNetworkReportResponse> responseList;
                if (task.authType == 3) {
                    responseList = downloadAdmobApi(task, cfg.adtClientId, cfg.adtClientSecret, err);
                } else {// 4:AdMobAPI-自有账号全手动配置
                    responseList = downloadAdmobApi(task, task.adnApiKey, task.userSignature, err);
                }
                if (!CollectionUtils.isEmpty(responseList) && err.length() == 0) {
                    task.step = 2;
                    error = saveAdmobResponseData(task, responseList);
                } else {
                    error = err.toString();
                }
            } else {// Use AdSense API
                if (task.authType == 0) {//0:开发者账号授权拉取
                    AuthAdmob auth = new AuthAdmob(cfg.proxy, cfg.authDir, task.adnApiKey, task.adnAppToken,
                            task.credentialPath, cfg.authDomain);
                    res = downloadAdsense(auth.getAdSense(), task.userId, task.day, err);
                } else if (task.authType == 1){//1:adt账号联合登录授权方式
                    res = downloadAdsenseWithOwnAccount(task, err);
                } else {//2:无需授权，开发者自有账号拉取
                    res = downloadAdsenseWithDevAccount(task, err);
                }
                if (res != null && !res.isEmpty() && err.length() == 0) {
                    task.step = 2;
                    error = saveResponseData(task, res);
                    if (StringUtils.isBlank(error)) {
                        task.step = 3;
                        error = savePrepareReportData(task);
                        if (StringUtils.isBlank(error)) {
                            task.step = 4;
                            error = reportLinkedToStat(task, task.userId);
                        }
                    }
                } else {
                    error = err.toString();
                }
            }
            if (StringUtils.isBlank(error)) {
                task.step = 3;
                error = savePrepareReportData(task);
                if (StringUtils.isBlank(error)) {
                    task.step = 4;
                    error = reportLinkedToStat(task, task.userId);
                }
            }

            int status = getStatus(error);
            error = convertMsg(error);
            if (task.runCount >= 4 && status != 2) {
                updateAccountException(jdbcTemplate, task, error);
                LOG.error("[Admob] executeTask error,run count:{},taskId:{},msg:{}", task.runCount + 1, task.id, error);
            } else {
                updateTaskStatus(jdbcTemplate, task.id, status, error);
            }
            LOG.info("[Admob] executeTask end, taskId:{}, cost:{}", task.id, System.currentTimeMillis() - start);
        } catch (Exception e) {
            updateTaskStatus(jdbcTemplate, task.id, 3, e.getMessage());
            LOG.error("[Admob] excuteTask error,taskId:{}",  task.id, e);
        }
    }

    private List<GenerateNetworkReportResponse> downloadAdmobApi(ReportTask task, String clientId, String clientSecret, StringBuilder err) {
        LOG.info("[Admob] download start, taskId:{}", task.id);
        long start = System.currentTimeMillis();
        try {
            NetHttpTransport transport = new NetHttpTransport.Builder().setProxy(cfg.proxy).build();
            GoogleCredential credential = new GoogleCredential.Builder()
                    .setJsonFactory(JacksonFactory.getDefaultInstance())
                    .setTransport(transport)
                    .setClientSecrets(clientId, clientSecret)
                    .build()
                    .setRefreshToken(task.adnAppToken);

            AdMob admob = new AdMob.Builder(transport, JSON_FACTORY, credential).build();
            /* AdMob API only supports the account default timezone and "America/Los_Angeles", see
             * https://developers.google.com/admob/api/v1/reference/rest/v1/accounts.networkReport/generate
             * for more information.
             */
            //String timeZone = "America/Los_Angeles";

            // Specify date range.
            LocalDateTime date = LocalDateTime.parse(task.day + " 00:00:00", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
            Date startDate = Util.toDate(ZonedDateTime.of(date, ZoneOffset.UTC));
            //DateUtils.toDate(ZonedDateTime.of(date.plusDays(1), ZoneOffset.UTC));
            DateRange dateRange = new DateRange().setStartDate(startDate).setEndDate(startDate);

            // Specify metrics.
            ImmutableList<String> metrics = ImmutableList.of("AD_REQUESTS", "MATCHED_REQUESTS", "IMPRESSIONS", "CLICKS", "ESTIMATED_EARNINGS");

            // Specify dimensions.
            ImmutableList<String> dimensions = ImmutableList.of("DATE", "APP", "AD_UNIT", "COUNTRY", "FORMAT", "PLATFORM");

            // Create network report specification.
            NetworkReportSpec reportSpec =
                    new NetworkReportSpec()
                            .setDateRange(dateRange)
                            //.setTimeZone(timeZone)
                            .setMetrics(metrics)
                            .setDimensions(dimensions);

            // Create network report request.
            GenerateNetworkReportRequest request = new GenerateNetworkReportRequest().setReportSpec(reportSpec);
            String accountName = "accounts/" + task.userId;
            // Get network report.
            InputStream response = admob.accounts()
                    .networkReport()
                    .generate(accountName, request)
                    .executeAsInputStream();

            return Arrays.asList(new JsonObjectParser(Utils.getDefaultJsonFactory())
                    .parseAndClose(response, StandardCharsets.UTF_8, GenerateNetworkReportResponse[].class));
        } catch (Exception e) {
            err.append(e.getMessage());
        }
        LOG.info("[Admob] download end, taskId:{}, cost:{}", task.id, System.currentTimeMillis() - start);
        return null;
    }

    private AdsenseReportsGenerateResponse downloadAdsense(AdSense adsense, String accountId, String day, StringBuilder err) {
        // Set up AdSense Management API client.
        if (adsense == null) {
            err.append("adsense is null");
            return null;
        }
        LOG.info("[Admob] download start, accountId:{}, day:{}", accountId, day);
        long start = System.currentTimeMillis();
        try {
            LocalDate date = LocalDate.parse(day, DateTimeFormatter.ofPattern("yyyy-MM-dd"));
            long count = date.toEpochDay() - LocalDateTime.now().toLocalDate().toEpochDay();
            String dayDif = count + "d";
            if (count == 0) {
                dayDif = "";
            }
            String startDate = "today" + dayDif;
            String endDate = "today" + dayDif;

            AdSense.Accounts.Reports.Generate request = adsense.accounts().reports().generate(accountId, startDate, endDate);
            request.setUseTimezoneReporting(true);
            request.setMetric(Arrays.asList("AD_REQUESTS", "AD_REQUESTS_COVERAGE", "AD_REQUESTS_CTR", "AD_REQUESTS_RPM",
                    "CLICKS", "COST_PER_CLICK", "EARNINGS", "INDIVIDUAL_AD_IMPRESSIONS", "INDIVIDUAL_AD_IMPRESSIONS_CTR",
                    "INDIVIDUAL_AD_IMPRESSIONS_RPM", "MATCHED_AD_REQUESTS", "MATCHED_AD_REQUESTS_CTR",
                    "MATCHED_AD_REQUESTS_RPM", "PAGE_VIEWS", "PAGE_VIEWS_CTR", "PAGE_VIEWS_RPM"));
            request.setDimension(Arrays.asList("DATE", "COUNTRY_CODE", "AD_CLIENT_ID", "AD_UNIT_ID"));

//         Sort by ascending date.
            request.setSort(Collections.singletonList("+DATE"));

            AdsenseReportsGenerateResponse response = request.execute();

            if (response == null || response.isEmpty()) {
                err.append("reponse is null or empty");
            }
            LOG.info("[Admob] download end, accountId:{}, day:{}, cost:{}", accountId, day, System.currentTimeMillis() - start);
            return response;
        } catch (Exception e) {
            err.append(e.getMessage());
        }
        return null;
    }

    private AdsenseReportsGenerateResponse downloadAdsenseWithDevAccount(ReportTask task, StringBuilder err) throws Exception {
        LOG.info("[Admob] download start, taskId:{}", task.id);
        long start = System.currentTimeMillis();
        try {
            NetHttpTransport transport = new NetHttpTransport.Builder().setProxy(cfg.proxy).build();
            GoogleCredential credential = new GoogleCredential.Builder()
                    .setJsonFactory(JacksonFactory.getDefaultInstance())
                    .setTransport(transport)
                    .setClientSecrets(task.adnApiKey, task.userSignature)
                    .build()
                    .setRefreshToken(task.adnAppToken);
            //.setFromTokenResponse(tokenResponse);

            AdSense adsense = new AdSense.Builder(transport,
                    JacksonFactory.getDefaultInstance(), credential)
                    .setApplicationName("google_api")
                    .build();
            LocalDate date = LocalDate.parse(task.day, DateTimeFormatter.ofPattern("yyyy-MM-dd"));
            long count = date.toEpochDay() - LocalDateTime.now().toLocalDate().toEpochDay();
            String dayDif = count + "d";
            if (count == 0) {
                dayDif = "";
            }
            String startDate = "today" + dayDif;
            String endDate = "today" + dayDif;
            AdSense.Reports.Generate request = adsense.reports().generate(startDate, endDate);
            request.setUseTimezoneReporting(true);
            request.setMetric(Arrays.asList("AD_REQUESTS", "AD_REQUESTS_COVERAGE", "AD_REQUESTS_CTR", "AD_REQUESTS_RPM",
                    "CLICKS", "COST_PER_CLICK", "EARNINGS", "INDIVIDUAL_AD_IMPRESSIONS", "INDIVIDUAL_AD_IMPRESSIONS_CTR",
                    "INDIVIDUAL_AD_IMPRESSIONS_RPM", "MATCHED_AD_REQUESTS", "MATCHED_AD_REQUESTS_CTR",
                    "MATCHED_AD_REQUESTS_RPM", "PAGE_VIEWS", "PAGE_VIEWS_CTR", "PAGE_VIEWS_RPM"));
            request.setDimension(Arrays.asList("DATE", "COUNTRY_CODE", "AD_CLIENT_ID", "AD_UNIT_ID"));
//         Sort by ascending date.
            request.setSort(Collections.singletonList("+DATE"));

            AdsenseReportsGenerateResponse response = request.execute();
            if (response == null || response.isEmpty()) {
                err.append("reponse is null or empty");
            }
            LOG.info("[Admob] download end, taskId:{}, cost:{}", task.id, System.currentTimeMillis() - start);
            return response;
        } catch (Exception e) {
            err.append(e.getMessage());
        }
        return null;
    }

    private AdsenseReportsGenerateResponse downloadAdsenseWithOwnAccount(ReportTask task, StringBuilder err) throws Exception {
        LOG.info("[Admob] download start, taskId:{}", task.id);
        long start = System.currentTimeMillis();
        try {
            NetHttpTransport transport = new NetHttpTransport.Builder().setProxy(cfg.proxy).build();
            GoogleCredential credential = new GoogleCredential.Builder()
                    .setJsonFactory(JacksonFactory.getDefaultInstance())
                    .setTransport(transport)
                    .setClientSecrets(cfg.adtClientId, cfg.adtClientSecret)
                    .build()
                    .setRefreshToken(task.adnAppToken);
            //.setFromTokenResponse(tokenResponse);

            AdSense adsense = new AdSense.Builder(transport,
                    JacksonFactory.getDefaultInstance(), credential)
                    .setApplicationName("google_api")
                    .build();
            LocalDate date = LocalDate.parse(task.day, DateTimeFormatter.ofPattern("yyyy-MM-dd"));
            long count = date.toEpochDay() - LocalDateTime.now().toLocalDate().toEpochDay();
            String dayDif = count + "d";
            if (count == 0) {
                dayDif = "";
            }
            String startDate = "today" + dayDif;
            String endDate = "today" + dayDif;
            AdSense.Reports.Generate request = adsense.reports().generate(startDate, endDate);
            request.setUseTimezoneReporting(true);
            request.setMetric(Arrays.asList("AD_REQUESTS", "AD_REQUESTS_COVERAGE", "AD_REQUESTS_CTR", "AD_REQUESTS_RPM",
                    "CLICKS", "COST_PER_CLICK", "EARNINGS", "INDIVIDUAL_AD_IMPRESSIONS", "INDIVIDUAL_AD_IMPRESSIONS_CTR",
                    "INDIVIDUAL_AD_IMPRESSIONS_RPM", "MATCHED_AD_REQUESTS", "MATCHED_AD_REQUESTS_CTR",
                    "MATCHED_AD_REQUESTS_RPM", "PAGE_VIEWS", "PAGE_VIEWS_CTR", "PAGE_VIEWS_RPM"));
            request.setDimension(Arrays.asList("DATE", "COUNTRY_CODE", "AD_CLIENT_ID", "AD_UNIT_ID"));
//         Sort by ascending date.
            request.setSort(Collections.singletonList("+DATE"));

            AdsenseReportsGenerateResponse response = request.execute();
            if (response == null || response.isEmpty()) {
                err.append("reponse is null or empty");
            }
            LOG.info("[Admob] download end, taskId:{}, cost:{}", task.id, System.currentTimeMillis() - start);
            return response;
        } catch (Exception e) {
            err.append(e.getMessage());
        }
        return null;
    }

    private String saveResponseData(ReportTask task, AdsenseReportsGenerateResponse response) {

        String day = response.getEndDate();
        String sql_delete = "DELETE FROM report_admob WHERE day=? AND account_key =?";
        try {
            jdbcTemplate.update(sql_delete, day, task.userId);
        } catch (Exception e) {
            return String.format("delete report_admob error,msg:%s", e.getMessage());
        }
        LOG.info("[Admob] saveResponseData start, taskId:{}, day:{}", task.id, day);
        long start = System.currentTimeMillis();
        String error = "";

        if (response.getHeaders() == null || response.getHeaders().size() != 20) {
            return "Response headers is null or header size is not 20";
        }

        List<String> columns = new ArrayList<>(response.getHeaders().size());
        List<String> columnMark = new ArrayList<>(response.getHeaders().size());
        for (int i = 0; i < response.getHeaders().size(); i++) {
            String name = response.getHeaders().get(i).getName();
            if ("DATE".equalsIgnoreCase(name)) {
                columns.add("day");
            } else if ("COUNTRY_CODE".equalsIgnoreCase(name)) {
                columns.add("country");
            } else {
                columns.add(name.toLowerCase());
            }
            columnMark.add("?");
        }

        String sql_insert = String.format("INSERT into report_admob(%s,account_key) VALUES(%s,?)",
                StringUtils.join(columns, ","), StringUtils.join(columnMark, ","));

        List<List<String>> rows = response.getRows();
        if (rows == null || rows.isEmpty()) {
            return "data is null";
        }
        // Display results.
        List<Object[]> lsParm = new ArrayList<>();
        try {
            out:
            for (List<String> row : rows) {
                row.add(task.userId);
                Object[] params = row.toArray();
                for (int i = 0; i < params.length; i++) {
                    if (params[i] == null || "".equals(params[i]))
                        params[i] = 0;
                    if (NumberUtils.isDigits(params[i].toString()) && Float.parseFloat(params[i].toString()) < 0)
                        continue out;
                }
                if (lsParm.size() == 1000) {
                    jdbcTemplate.batchUpdate(sql_insert, lsParm);
                    lsParm.clear();
                }
                lsParm.add(params);
            }
            if (!lsParm.isEmpty()) {
                jdbcTemplate.batchUpdate(sql_insert, lsParm);
            }
        } catch (Exception e) {
            error = String.format("insert into report_admob error,msg:%s", e.getMessage());
        }
        LOG.info("[Admob] saveResponseData end, taskId:{}, day:{}, cost:{}", task.id, day, System.currentTimeMillis() - start);
        return error;
    }

    private String saveAdmobResponseData(ReportTask task, List<GenerateNetworkReportResponse> responseList) {
        if (CollectionUtils.isEmpty(responseList)) {
            return "data is null";
        }
        String day = task.day;
        String sql_delete = "DELETE FROM report_admob WHERE day=? AND account_key=?";
        try {
            jdbcTemplate.update(sql_delete, day, task.userId);
        } catch (Exception e) {
            return String.format("delete report_admob error,msg:%s", e.getMessage());
        }
        LOG.info("[Admob] saveResponseData start, taskId:{}", task.id);
        long start = System.currentTimeMillis();
        String error = "";
        try {
            String[] dimensions = {"APP", "AD_UNIT", "COUNTRY"};
            String[] metrics = {"AD_REQUESTS", "MATCHED_REQUESTS", "IMPRESSIONS", "CLICKS", "ESTIMATED_EARNINGS"};
            List<Object[]> insertParams = new ArrayList<>();
            String insertSql = "insert into report_admob(day,ad_client_id,ad_unit_id,country,ad_requests,matched_ad_requests,individual_ad_impressions,clicks,earnings,account_key) values(?,?,?,?,?,?,?,?,?,?)";
            for (GenerateNetworkReportResponse record : responseList) {
                ReportRow row = record.getRow();
                if (row == null) continue;
                Object[] rowData = new Object[10];
                Map<String, ReportRowDimensionValue> rowDimensionValueMap = row.getDimensionValues();
                int index = 1;
                rowData[0] = task.day;
                for (String dimension : dimensions) {
                    ReportRowDimensionValue value = rowDimensionValueMap.get(dimension);
                    if (value == null) {
                        rowData[index] = "";
                    } else {
                        rowData[index] = value.getValue();
                    }
                    index++;
                }
                Map<String, ReportRowMetricValue> rowMetricValueMap = row.getMetricValues();
                for (String metric : metrics) {
                    ReportRowMetricValue value = rowMetricValueMap.get(metric);
                    if (value == null) {
                        rowData[index] = 0;
                    } else {
                        if ("ESTIMATED_EARNINGS".equals(metric)) {
                            rowData[index] = value.getMicrosValue() == null ? 0 : value.getMicrosValue().doubleValue()/1000000.0;
                        } else {
                            rowData[index] = value.getIntegerValue() == null ? 0 : value.getIntegerValue().intValue();
                        }
                    }
                    index++;
                }
                rowData[9] = task.userId;
                insertParams.add(rowData);
            }
            if (!insertParams.isEmpty()) {
                jdbcTemplate.batchUpdate(insertSql, insertParams);
            } else {
                error = "data is null";
            }
        } catch (Exception e) {
            error = String.format("insert into report_admob error,msg:%s", e.getMessage());
        }
        LOG.info("[Admob] saveResponseData end, taskId:{}, cost:{}", task.id, System.currentTimeMillis() - start);
        return error;
    }

    public String savePrepareReportData(ReportTask task) {
        LOG.info("[Admob] savePrepareReportData start, taskId:{}, day", task.id);
        long start = System.currentTimeMillis();
        String error;
        try {
            String reportDay = task.day;
            List<Map<String, Object>> instanceInfoList = getInstanceList(task.reportAccountId);

            if (instanceInfoList.isEmpty())
                return "instance is null";

            LocalDate dataDay = LocalDate.parse(task.day, DATEFORMAT_YMD);
            Map<String, Map<String, Object>> placements = new HashMap<>();
            String pubFormatId = "ca-app-" + task.userId;
            for (Map<String, Object> ins : instanceInfoList) {
                if (task.authType > 2) {// 3,4admob API数据关联使用adn_app_key+"-"+placement_key
                    String key = MapHelper.getString(ins, "adn_app_key") + "-" + MapHelper.getString(ins, "placement_key");
                    putLinkKeyMap(placements, key, ins, dataDay);
                } else {//Adsense API数据关联使用admob publisher_id+placement_key
                    String key = pubFormatId + "-" + MapHelper.getString(ins, "placement_key").replace("/", ":");
                    putLinkKeyMap(placements, key, ins, dataDay);
                }
            }

            String dataSql = "select day,0 hour,country," +
                    "ad_unit_id adn_placement_key,concat(ad_client_id, '-', ad_unit_id) data_key," +
                    "sum(ad_requests) as api_request," +
                    "sum(individual_ad_impressions) as api_impr,sum(clicks) AS api_click," +
                    "sum(earnings) AS revenue,sum(matched_ad_requests) AS api_filled " +
                    " FROM report_admob where day=? and account_key=?" +
                    " group by day,country,ad_unit_id";

            List<ReportAdnData> oriDataList = jdbcTemplate.query(dataSql, ReportAdnData.ROWMAPPER, reportDay, task.userId);
            if (oriDataList.isEmpty())
                return "data is null";
            error = toAdnetworkLinked(task, task.userId, placements, oriDataList);
        } catch (Exception e) {
            error = String.format("savePrepareReportData error, msg:%s", e.getMessage());
        }
        LOG.info("[Admob] savePrepareReportData end, taskId:{}, cost:{}", task.id, System.currentTimeMillis() - start);
        return error;
    }
}
