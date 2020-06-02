// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.adc.service.admob;

import com.adtiming.om.adc.dto.ReportAdnData;
import com.adtiming.om.adc.dto.ReportTask;
import com.adtiming.om.adc.service.AdnBaseService;
import com.adtiming.om.adc.service.AppConfig;
import com.adtiming.om.adc.util.MapHelper;
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
import java.util.stream.Collectors;

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
            AuthAdmob auth = new AuthAdmob(cfg.proxy, cfg.authDir, task.adnApiKey, task.adnAppToken, task.credentialPath, cfg.authDomain);
            AdsenseReportsGenerateResponse res = download(auth.getAdSense(), task.userId, task.day, err);
            if (res != null && !res.isEmpty() && err.length() == 0) {
                error = saveResponseData(res, task.userId);
                if (StringUtils.isBlank(error)) {
                    error = savePrepareReportData(task, task.day, task.userId);
                    if (StringUtils.isBlank(error)) {
                        String accountKey = "ca-app-" + task.userId;
                        error = reportLinkedToStat(task, accountKey);
                    }
                }
            } else {
                error = err.toString();
            }
            int status = StringUtils.isBlank(error) || "data is null".equals(error) ? 2 : 3;
            if (task.runCount > 5 && status != 2) {
                LOG.error("[Admob] executeTask error,run count:{},taskId:{},msg:{}", task.runCount + 1, task.id, error);
            }
            updateTaskStatus(jdbcTemplate, task.id, status, error);

            LOG.info("[Admob] executeTask end, taskId:{}, cost:{}", task.id, System.currentTimeMillis() - start);
        } catch (Exception e) {
            updateTaskStatus(jdbcTemplate, task.id, 3, e.getMessage());
            LOG.error("[Admob] excuteTask error,taskId:{}", task.id, e);
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
    }

    private String saveResponseData(AdsenseReportsGenerateResponse response, String pubId) {

        String day = response.getEndDate();
        pubId = "ca-app-" + pubId;
        String sql_delete = "DELETE FROM report_admob WHERE day=? AND ad_client_id =?";
        try {
            jdbcTemplate.update(sql_delete, day, pubId);
        } catch (Exception e) {
            return String.format("delete report_admob error,msg:%s", e.getMessage());
        }
        LOG.info("[Admob] saveResponseData start, accountId:{}, day", pubId, day);
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
        LOG.info("[Admob] saveResponseData end, accountId, day:{}, cost:{}", pubId, day, System.currentTimeMillis() - start);
        return error;
    }

    public String savePrepareReportData(ReportTask task, String reportDay, String pubId) {
        LOG.info("[Admob] savePrepareReportData start, taskId:{}, day", task.id);
        long start = System.currentTimeMillis();
        String error;
        try {
            String pubFormatId = "ca-app-" + pubId;
            String whereSql = String.format("b.report_app_id='%s'", pubId);
            String changeSql = String.format("(b.report_app_id='%s' or b.new_account_key='%s')", pubId, pubId);
            List<Map<String, Object>> instanceInfoList = getInstanceList(whereSql, changeSql);

            Map<String, Map<String, Object>> placements = instanceInfoList.stream().collect(Collectors.toMap(m -> MapHelper.getString(m, "placement_key").replace("/",":"), m -> m, (existingValue, newValue) -> existingValue));

            // instance's placement_key changed
            Set<Integer> insIds = instanceInfoList.stream().map(o->MapHelper.getInt(o, "instance_id")).collect(Collectors.toSet());
            List<Map<String, Object>> oldInstanceList = getOldInstanceList(insIds);
            if (!oldInstanceList.isEmpty()) {
                placements.putAll(oldInstanceList.stream().collect(Collectors.toMap(m ->
                        MapHelper.getString(m, "placement_key").replace("/",":"), m -> m, (existingValue, newValue) -> existingValue)));
            }
            if (instanceInfoList.isEmpty())
                return "data is null";

            String dataSql = "select day,0 hour,country," +
                    "ad_unit_id adn_placement_key,ad_unit_id data_key," +
                    "sum(ad_requests) as api_requests," +
                    "sum(individual_ad_impressions) as api_impr,sum(clicks) AS api_click," +
                    "sum(earnings) AS revenue,sum(matched_ad_requests) AS api_filled " +
                    " FROM report_admob where day=? and ad_client_id=?" +
                    " group by day,country,ad_unit_id";

            List<ReportAdnData> oriDataList = jdbcTemplate.query(dataSql, ReportAdnData.ROWMAPPER, reportDay, pubFormatId);
            if (oriDataList.isEmpty())
                return "data is null";
            error = toAdnetworkLinked(task, pubFormatId, placements, oriDataList);
        } catch (Exception e) {
            error = String.format("savePrepareReportData error, msg:%s", e.getMessage());
        }
        LOG.info("[Admob] savePrepareReportData end, accountId, day:{}, cost:{}", pubId, reportDay, System.currentTimeMillis() - start);
        return error;
    }
}
