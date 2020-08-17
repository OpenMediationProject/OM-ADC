// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.adc.service.unity;

import com.adtiming.om.adc.dto.ReportAdnData;
import com.adtiming.om.adc.dto.ReportTask;
import com.adtiming.om.adc.service.AdnBaseService;
import com.adtiming.om.adc.service.AppConfig;
import com.adtiming.om.adc.util.DateTimeFormat;
import com.adtiming.om.adc.util.MapHelper;
import com.adtiming.om.adc.util.MyHttpClient;
import com.adtiming.om.adc.util.Util;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class DownloadUnity extends AdnBaseService {

    private static final Logger LOG = LogManager.getLogger();

    @Resource
    private JdbcTemplate jdbcTemplate;

    @Resource
    private AppConfig cfg;

    @Value("${download.dir}")
    private String downloadDir;

    @Override
    public void setAdnInfo() {
        this.adnId = 4;
        this.adnName = "Unity";
    }

    @Override
    public void executeTaskImpl(ReportTask o) {
        try {
            downloadData(o);
        } catch (Exception e) {
            LOG.error("[{}] executeTaskImpl error, taskId:{}", this.adnName, o.id, e);
        }
    }

    private void downloadData(ReportTask task) {
        String appKey = task.adnApiKey;
        String day = task.day;
        String hour = task.hour < 10 ? "0" + task.hour : String.valueOf(task.hour);
        if (StringUtils.isBlank(appKey)) {
            LOG.error("[Unity] appKey is null");
            return;
        }
        LOG.info("[Unity] download start,appKey:{}, day:{}, hour:{}", appKey, day, hour);
        long start = System.currentTimeMillis();
        try {
            updateTaskStatus(jdbcTemplate, task.id, 1, "");

            boolean isHour = task.timeDimension == 0;
            String startDate = String.format("%sT%s:00:00.000Z", day, isHour ? hour : "00");
            String endDate = String.format("%sT%s:59:59.999Z", day, isHour ? hour : "23");
            String scale = isHour ? "hour" : "day";
            StringBuilder sb = new StringBuilder();
            String error;
            task.step = 1;
            if (StringUtils.isNoneBlank(task.userId)) {
                error = excuteByNewApi(task, startDate, endDate, scale);
            } else {
                error = excuteByOldApi(task, hour, startDate, endDate, scale);
            }
            if (StringUtils.isBlank(error)) {
                task.step = 3;
                error = savePrepareReportData(task, day, task.hour, task.adnApiKey);
                if (StringUtils.isBlank(error)) {
                    task.step = 4;
                    error = reportLinkedToStat(task, task.adnApiKey);
                }
            }
            int status = getStatus(error);
            error = convertMsg(error);
            if (task.runCount > 5 && status != 2) {
                updateAccountException(jdbcTemplate, task, error);
                LOG.error("[Unity] executeTaskImpl error,run count:{},taskId:{},msg:{}", task.runCount + 1, task.id, error);
            } else {
                updateTaskStatus(jdbcTemplate, task.id, status, error);
            }
        } catch (Exception e) {
            updateTaskStatus(jdbcTemplate, task.id, 3, e.getMessage());
        }
        //executeTaskIds.remove(task.id);
        LOG.info("[Unity] download end,appKey:{}, day:{}, hour:{}, cost:{}", appKey, day, hour, System.currentTimeMillis() - start);
    }

    private String excuteByOldApi(ReportTask task, String hour, String startDate, String endDate, String scale) {
        String url = String.format("https://gameads-admin.applifier.com/stats/monetization-api?apikey=%s&splitBy=source,zone,country&fields=adrequests,available,offers,started,views,revenue&start=%s&end=%s&scale=%s", task.adnApiKey, startDate, endDate, scale);
        updateReqUrl(jdbcTemplate, task.id, url);
        StringBuilder sb = new StringBuilder();
        String file_path = DownCsvFile(url, task.day, hour, task.adnApiKey, sb);
        String error;
        if (StringUtils.isNotBlank(file_path)) {
            error = ReadCsvFile(task, file_path, task.day, hour, task.adnApiKey);
            if (StringUtils.isBlank(error)) {
                return "";
            }
        } else {
            error = sb.toString();
        }
        return error;
    }

    private String excuteByNewApi(ReportTask task, String startDate, String endDate, String scale) {
        String url = String.format("https://monetization.api.unity.com/stats/v1/operate/organizations/%s?apikey=%s&fields=adrequest_count,start_count,view_count,available_sum,revenue_sum&groupBy=game,platform,placement,country&start=%s&end=%s&scale=%s", task.userId, task.adnApiKey, startDate, endDate, scale);
        updateReqUrl(jdbcTemplate, task.id, url);
        StringBuilder sb = new StringBuilder();
        String error;
        String jsonData = getJsonData(url, sb);
        if (StringUtils.isNoneBlank(jsonData) && sb.length() == 0) {
            error = jsonToDB(task, jsonData);
        } else {
            error = sb.toString();
        }
        return error;
    }

    private String getJsonData(String url, StringBuilder sb) {
        String json = "";
        HttpEntity entity = null;
        try {
            HttpGet httpGet = new HttpGet(url);
            RequestConfig requestConfig = RequestConfig.custom()
                    .setSocketTimeout(5 * 60 * 1000)//5分钟
                    .setProxy(cfg.httpProxy)
                    .setCookieSpec(CookieSpecs.IGNORE_COOKIES)
                    .build();
            httpGet.setConfig(requestConfig);
            httpGet.setHeader("Accept", "application/json");
            //发送Post,并返回一个HttpResponse对象
            HttpResponse response = MyHttpClient.getInstance().execute(httpGet);
            entity = response.getEntity();
            StatusLine sl = response.getStatusLine();
            if (entity == null) {
                sb.append("response data is null");
                return json;
            }
            if (sl.getStatusCode() != 200) {//如果状态码为200,就是正常返回
                sb.append(String.format("response status code is not 200,statusCode:%d,msg:%s", sl.getStatusCode(), EntityUtils.toString(entity)));
                return json;
            }

            json = EntityUtils.toString(entity);
        } catch (Exception ex) {
            sb.append(String.format("getJsonData error,error:%s", ex.getMessage()));
        } finally {
            EntityUtils.consumeQuietly(entity);
        }
        return json;
    }

    private String jsonToDB(ReportTask task, String jsonData) {
        task.step = 2;
        long start = System.currentTimeMillis();
        LOG.info("[Unity] jsonToDB start,taskId:{}", task.id);
        String error = "";
        int count = 0;
        try {
            String deleteSql = String.format("delete from report_unity where day=? and app_key=? %s", task.timeDimension == 0 ? "and hour=" + task.hour : "");
            jdbcTemplate.update(deleteSql, task.day, task.adnApiKey);
        } catch (Exception e) {
            return String.format("delete report_unity error,%s", e.getMessage());
        }
        try {
            JSONArray array = JSON.parseArray(jsonData);
            if (!array.isEmpty()) {
                List<Object[]> insertParams = new ArrayList<>();
                count = array.size();
                String insertSql = "INSERT into report_unity (day,hour,source_game_id,source_game_name,source_zone,country," +
                        "country_tier,adrequests,available,offers,started,views,revenue,app_key)  VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
                for (int i = 0; i < count; i++) {
                    JSONObject obj = array.getJSONObject(i);
                    String reportDay;
                    String reportHour;
                    try {
                        LocalDateTime time = LocalDateTime.parse(Util.getJSONString(obj, "timestamp"), DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", Locale.ENGLISH));
                        reportDay = DateTimeFormat.DAY_FORMAT.format(time);
                        reportHour = DateTimeFormat.HOUR_FORMAT.format(time);
                    } catch (Exception e) {
                        reportDay = task.day;
                        reportHour = String.valueOf(task.hour);
                    }
                    insertParams.add(new Object[] {reportDay, reportHour,
                            Util.getJSONString(obj, "source_game_id"), Util.getJSONString(obj, "source_name"),
                            Util.getJSONString(obj, "placement"),
                            Util.getJSONString(obj, "country"), 0, Util.getJSONInt(obj, "adrequest_count"),
                            Util.getJSONInt(obj, "available_sum"), 0, Util.getJSONInt(obj, "start_count"),
                            Util.getJSONInt(obj, "view_count"), Util.getJSONDecimal(obj, "revenue_sum"),
                            task.adnApiKey
                    });
                    if (insertParams.size() >= 1000) {
                        jdbcTemplate.batchUpdate(insertSql, insertParams);
                        insertParams.clear();
                    }
                }
                if (!insertParams.isEmpty()) {
                    jdbcTemplate.batchUpdate(insertSql, insertParams);
                }
            }
        } catch (Exception e) {
            error = e.getMessage();
        }
        LOG.info("[Unity] insert report_unity finished, taskId:{}, count:{}, cost:{}", task.id, count, System.currentTimeMillis() - start);
        return error;
    }

    private String DownCsvFile(String url, String day, String hour, String appKey, StringBuilder sb) {
        String filePath = "";
        HttpEntity entity = null;
        try {
            HttpGet httpGet = new HttpGet(url);
            LOG.info("[Unity] request url:{}, day:{}, hour:{}, appKey:{}", url, day, hour, appKey);
            httpGet.setConfig(RequestConfig.custom().setCookieSpec(CookieSpecs.IGNORE_COOKIES).setProxy(cfg.httpProxy).build());
            HttpResponse response = MyHttpClient.getInstance().execute(httpGet);
            entity = response.getEntity();
            StatusLine sl = response.getStatusLine();
            entity = response.getEntity();
            if (sl.getStatusCode() != 200) {
                sb.append(String.format("request report response statusCode:%d,msg:%s", sl.getStatusCode(), entity == null ? "" : EntityUtils.toString(entity)));
                return filePath;
            }
            if (entity == null) {
                LOG.info("[Unity] response data is null,appKey:{},day:{},hour:{}", appKey, day, hour);
                sb.append("response data is null");
                return filePath;
            }
            if (entity.isStreaming()) {
                try (InputStream instream = entity.getContent()) {
                    if (instream != null) {
                        String path = String.format("/unity/unity-%s-%s-%s.csv", day, hour, appKey);
                        File dst = new File(downloadDir, path);
                        File dst_dir = dst.getParentFile();
                        FileUtils.forceMkdir(dst_dir);
                        FileUtils.copyInputStreamToFile(instream, dst);
                        instream.close();
                        filePath = downloadDir + path;
                    }
                } catch (Exception e) {
                    sb.append(String.format("Download csv error,error:%s", e.getMessage()));
                    LOG.info("[Unity] Download csv error,appKey:{},day:{},hour:{}", appKey, day, hour, e);
                    return filePath;
                }
                LOG.info("[Unity] Download csv finished,appKey:{},day:{},hour:{}", appKey, day, hour);
                return filePath;
            } else {
                sb.append("response entity is not a stream");
            }
            LOG.info("[Unity] data is null,appKey:{},day:{},hour:{}", appKey, day, hour);
        } catch (Exception ex) {
            sb.append(String.format("Download csv error,error:%s", ex.getMessage()));
            LOG.info("[Unity] Download csv error,appKey:{},day:{},hour:{}", appKey, day, hour, ex);
        } finally {
            EntityUtils.consumeQuietly(entity);
        }
        return filePath;
    }

    private String ReadCsvFile(ReportTask task, String csvFilePath, String day, String hour, String appKey) {
        String fileEncoder = "UTF-8";//读取文件编码方式，主要是为了解决中文乱码问题。
        //BufferedReader in = null;
        String sql_delete = String.format("delete from report_unity where day=? and app_key=? %s", task.timeDimension == 0 ? "and hour=" + hour : "");
        long start = System.currentTimeMillis();
        LOG.info("[Unity] insert report_unity start,appKey:{},day:{},hour:{}", appKey, day, hour);
        try {
            jdbcTemplate.update(sql_delete, day, appKey);
        } catch (Exception e) {
            return String.format("delete report_unity error,%s", e.getMessage());
        }

        String error = "";
        int count = 0;
        try (BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(csvFilePath), fileEncoder))) {
            String insertSql = "INSERT INTO report_unity (day,source_game_id,source_game_name,source_zone,country," +
                    "country_tier,adrequests,available,offers,started,views,revenue,hour,app_key)  VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
            String rec;
            boolean haveData = false;
            List<Object[]> lsParm = new ArrayList<>(1000);
            while ((rec = in.readLine()) != null) {
                count++;
                if (count > 1) {
                    haveData = true;
                    Object[] arr = rec.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1);//双引号内的逗号不分割 双引号外的逗号进行分割
                    Object[] params = new Object[arr.length + 2];
                    for (int i = 0; i < arr.length; i++) {
                        if (i == 0) {
                            String reportDay;
                            String reportHour;
                            try {
                                LocalDateTime time = LocalDateTime.parse(arr[0].toString().replace("\"", ""), DateTimeFormat.TIME_FORMAT);
                                reportDay = DateTimeFormat.DAY_FORMAT.format(time);
                                reportHour = DateTimeFormat.HOUR_FORMAT.format(time);
                            } catch (Exception e) {
                                reportDay = day;
                                reportHour = hour;

                            }
                            params[0] = reportDay;
                            params[arr.length] = reportHour;
                            params[arr.length + 1] = appKey;
                        }
                        if (params[i] == null || "".equals(arr[i].toString().replace("\"", "")))
                            params[i] = 0;
                        if (i != 0)
                            params[i] = arr[i].toString().replace("\"", "");
                    }
                    lsParm.add(params);
                    if (lsParm.size() == 1000) {
                        jdbcTemplate.batchUpdate(insertSql, lsParm);
                        lsParm.clear();
                    }
                }
            }
            if (!haveData) {
                LocalDateTime lastDay = LocalDateTime.parse(day + " " + hour + ":00:00", DateTimeFormat.TIME_FORMAT);
                java.time.Duration duration = java.time.Duration.between(lastDay, LocalDateTime.now());
                int hours = (int) duration.toHours();
                if (hours > 23) {
                    LOG.warn("[Unity] Failed to pull data for more than 24 hours,day:{},hour:{},appKey:{}", day, hour, appKey);
                }
                return "data is null";
            }
            if (!lsParm.isEmpty()) {
                jdbcTemplate.batchUpdate(insertSql, lsParm);
            }
        } catch (Exception e) {
            error = String.format("delete report_unity error,%s", e.getMessage());
        }
        LOG.info("[Unity] insert report_unity finished,appKey:{},day:{},hour:{}, count:{}, cost:{}", appKey, day, hour, count, System.currentTimeMillis() - start);
        return error;
    }

    private String savePrepareReportData(ReportTask task, String reportDay, int reportHour, String appKey) {
        LOG.info("[Unity] savePrepareReportData start, taskId:{}", task.id);
        long start = System.currentTimeMillis();
        String error;
        try {
            List<Map<String, Object>> instanceInfoList = getInstanceList(task.reportAccountId);

            Map<String, Map<String, Object>> placements = instanceInfoList.stream().collect(Collectors.toMap(m ->
                    MapHelper.getString(m, "adn_app_key") + "_" + MapHelper.getString(m, "placement_key"), m -> m, (existingValue, newValue) -> existingValue));
            placements.putAll(instanceInfoList.stream().collect(Collectors.toMap(m ->
                    MapHelper.getString(m, "adn_app_key") + "_" + MapHelper.getString(m, "adn_app_key"), m -> m, (existingValue, newValue) -> existingValue)));

            // instance's placement_key changed
            Set<Integer> insIds = instanceInfoList.stream().map(o->MapHelper.getInt(o, "instance_id")).collect(Collectors.toSet());
            List<Map<String, Object>> oldInstanceList = getOldInstanceList(insIds);
            if (!oldInstanceList.isEmpty()) {
                placements.putAll(oldInstanceList.stream().collect(Collectors.toMap(m ->
                        MapHelper.getString(m, "adn_app_key") + "_" + MapHelper.getString(m, "placement_key"), m -> m, (existingValue, newValue) -> existingValue)));
            }

            String dataSql = String.format("select day,hour,country,case when source_zone is null or source_zone = '' then concat(source_game_id,'_',source_game_id) else concat(source_game_id,'_',source_zone) end data_key," +
                    "sum(adrequests) api_request,sum(available) api_filled," +
                    "sum(started) api_impr,sum(views) api_click," +
                    "sum(started) api_video_start,sum(views) api_video_complete," +
                    "sum(revenue) revenue " +
                    " from report_unity" +
                    " where day=? and app_key=? %s" +
                    " group by hour,day,country,source_game_id,source_game_name,source_zone", task.timeDimension == 0 ? "and hour=" + reportHour : "");

            List<ReportAdnData> oriDataList = jdbcTemplate.query(dataSql, ReportAdnData.ROWMAPPER, reportDay, appKey);

            if (oriDataList.isEmpty())
                return "data is empty";

            error = toAdnetworkLinked(task, appKey, placements, oriDataList);
        } catch (Exception e) {
            //log.error("[Unity] savePrepareReportData error", e);
            error = String.format("savePrepareReportData error:%s", e.getMessage());
        }
        LOG.info("[Unity] savePrepareReportData end, appKey:{}, day:{}, hour:{}, cost:{}", appKey, reportDay, reportHour, System.currentTimeMillis() - start);
        return error;
    }
}

