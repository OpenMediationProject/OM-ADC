// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.adc.service.admob;

import com.adtiming.om.adc.dto.ReportAdnData;
import com.adtiming.om.adc.dto.ReportTask;
import com.adtiming.om.adc.service.AdnBaseService;
import com.adtiming.om.adc.service.AppConfig;
import com.adtiming.om.adc.util.MapHelper;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.adsense.AdSense;
import com.google.api.services.adsense.model.AdsenseReportsGenerateResponse;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

@Service
public class DownloadAdmob extends AdnBaseService {

    private static final Logger LOG = LogManager.getLogger();

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
            if (task.authType == 0) {//0:开发者账号授权拉取
                AuthAdmob auth = new AuthAdmob(cfg.proxy, cfg.authDir, task.adnApiKey, task.adnAppToken, task.credentialPath, cfg.authDomain);
                res = download(auth.getAdSense(), task.userId, task.day, err);
            } else if (task.authType == 1){//1:adt账号联合登录授权方式
                res = downloadWithOwnAccount(task, err);
            } else {//2:无需授权，开发者自有账号拉取
                res = downloadWithDevAccount(task, err);
            }
            if (res != null && !res.isEmpty() && err.length() == 0) {
                task.step = 2;
                String accountKey = "ca-app-" + task.userId;
                error = saveResponseData(res, accountKey);
                if (StringUtils.isBlank(error)) {
                    task.step = 3;
                    error = savePrepareReportData(task, accountKey);
                    if (StringUtils.isBlank(error)) {
                        task.step = 4;
                        error = reportLinkedToStat(task, accountKey);
                    }
                }
            } else {
                error = err.toString();
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

    private AdsenseReportsGenerateResponse download(AdSense adsense, String accountId, String day, StringBuilder err) throws IOException {
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

    private AdsenseReportsGenerateResponse downloadWithDevAccount(ReportTask task, StringBuilder err) throws Exception {
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

    private AdsenseReportsGenerateResponse downloadWithOwnAccount(ReportTask task, StringBuilder err) throws Exception {
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

    private String saveResponseData(AdsenseReportsGenerateResponse response, String accountKey) {

        String day = response.getEndDate();
        String sql_delete = "DELETE FROM report_admob WHERE day=? AND ad_client_id =?";
        try {
            jdbcTemplate.update(sql_delete, day, accountKey);
        } catch (Exception e) {
            return String.format("delete report_admob error,msg:%s", e.getMessage());
        }
        LOG.info("[Admob] saveResponseData start, accountId:{}, day", accountKey, day);
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

        String sql_insert = String.format("INSERT into report_admob(%s) VALUES(%s)",
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
        LOG.info("[Admob] saveResponseData end, accountId, day:{}, cost:{}", accountKey, day, System.currentTimeMillis() - start);
        return error;
    }

    public String savePrepareReportData(ReportTask task, String pubId) {
        LOG.info("[Admob] savePrepareReportData start, taskId:{}, day", task.id);
        long start = System.currentTimeMillis();
        String error;
        try {
            String reportDay = task.day;
            String pubFormatId = "ca-app-" + pubId;
            List<Map<String, Object>> instanceInfoList = getInstanceList(task.reportAccountId);

            /*Map<String, Map<String, Object>> placements = instanceInfoList.stream().collect(Collectors.toMap(m ->MapHelper.getString(m, "placement_key").replace("/",":"), m -> m, (existingValue, newValue) -> existingValue));

            // instance's placement_key changed
            Set<Integer> insIds = instanceInfoList.stream().map(o->MapHelper.getInt(o, "instance_id")).collect(Collectors.toSet());
            List<Map<String, Object>> oldInstanceList = getOldInstanceList(insIds);
            if (!oldInstanceList.isEmpty()) {
                placements.putAll(oldInstanceList.stream().collect(Collectors.toMap(m ->
                        MapHelper.getString(m, "placement_key").replace("/",":"), m -> m, (existingValue, newValue) -> existingValue)));
            }*/

            if (instanceInfoList.isEmpty())
                return "instance is null";

            LocalDate dataDay = LocalDate.parse(task.day, DATEFORMAT_YMD);
            Map<String, Map<String, Object>> placements = new HashMap<>();
            for (Map<String, Object> ins : instanceInfoList) {
                String key = pubFormatId + "-" + MapHelper.getString(ins, "placement_key").replace("/", ":");
                putLinkKeyMap(placements, key, ins, dataDay);
            }

            String dataSql = "select day,0 hour,country," +
                    "ad_unit_id adn_placement_key,concat(ad_client_id, '-', ad_unit_id) data_key," +
                    "sum(ad_requests) as api_request," +
                    "sum(individual_ad_impressions) as api_impr,sum(clicks) AS api_click," +
                    "sum(earnings) AS revenue,sum(matched_ad_requests) AS api_filled " +
                    " FROM report_admob where day=? and ad_client_id=?" +
                    " group by day,country,ad_unit_id";

            List<ReportAdnData> oriDataList = jdbcTemplate.query(dataSql, ReportAdnData.ROWMAPPER, reportDay, pubId);
            if (oriDataList.isEmpty())
                return "data is null";
            error = toAdnetworkLinked(task, pubId, placements, oriDataList);
        } catch (Exception e) {
            error = String.format("savePrepareReportData error, msg:%s", e.getMessage());
        }
        LOG.info("[Admob] savePrepareReportData end, taskId:{}, cost:{}", task.id, System.currentTimeMillis() - start);
        return error;
    }
}
