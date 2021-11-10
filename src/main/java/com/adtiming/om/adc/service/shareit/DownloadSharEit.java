package com.adtiming.om.adc.service.shareit;

import com.adtiming.om.adc.dto.ReportAdnData;
import com.adtiming.om.adc.dto.ReportTask;
import com.adtiming.om.adc.service.AdnBaseService;
import com.adtiming.om.adc.service.AppConfig;
import com.adtiming.om.adc.util.Encrypter;
import com.adtiming.om.adc.util.MapHelper;
import com.adtiming.om.adc.util.MyHttpClient;
import com.adtiming.om.adc.util.ParamsBuilder;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
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
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URLEncoder;
import java.time.LocalDate;
import java.util.*;

@Service
public class DownloadSharEit extends AdnBaseService {

    private static final Logger LOG = LogManager.getLogger();
    private static final int PAGE_SIZE = 10000;

    @Resource
    private JdbcTemplate jdbcTemplateW;

    @Resource
    private AppConfig cfg;

    @Override
    public void setAdnInfo() {
        this.adnId = 27;
        this.adnName = "SharEit";
    }

    @Override
    public void executeTaskImpl(ReportTask o) {
        try {
            executeSharEitTask(o);
        } catch (Exception e) {
            LOG.error("[{}] executeTaskImpl error, taskId:{}", this.adnName, o.id, e);
        }
    }

    private void executeSharEitTask(ReportTask task) {
        String appToken = task.adnAppToken;
        String appId = task.adnAppId;
        String day = task.day;
        // Set up AdSense Management API client.
        if (StringUtils.isBlank(appToken)) {
            LOG.error("[SharEit] app token is null, taskId:{}", task.id);
            return;
        }
        if (StringUtils.isBlank(appId)) {
            LOG.error("[SharEit] app id is null, taskId:{}", task.id);
            return;
        }

        LOG.info("[SharEit] executeTaskImpl start, taskId:{}", task.id);
        long start = System.currentTimeMillis();
        updateTaskStatus(jdbcTemplateW, task.id, 1, "");
        String error = downLoadData(task, day);
        if (StringUtils.isBlank(error)) {
            task.step = 3;
            error = savePrepareReportData(task, day);
            if (StringUtils.isBlank(error)) {
                task.step = 4;
                error = reportLinkedToStat(task, appToken);
            }
        }

        int status = getStatus(error);
        error = convertMsg(error);
        if (task.runCount >= 4 && status != 2) {
            updateAccountException(jdbcTemplateW, task, error);
            LOG.warn("[SharEit] executeTaskImpl error,run count:{},taskId:{},msg:{}", task.runCount + 1, task.id, error);
        } else {
            updateTaskStatus(jdbcTemplateW, task.id, status, error);
        }
        LOG.info("[SharEit] executeTaskImpl end, skey:{}, secret:{}, day:{}, cost:{}", appToken, appId, day, System.currentTimeMillis() - start);
    }

    private String downLoadData(ReportTask task, String day) {
        String error = "";
        StringBuilder err = new StringBuilder();
        String appToken = task.adnAppToken;
        try {
            task.step = 1;
            LocalDate start = LocalDate.parse(day, DATEFORMAT_YMD);
            String startDate = start.format(DATEFORMAT_DAY);
            String endDate = start.plusDays(1).format(DATEFORMAT_DAY);
            String jsonData = downloadPageJsonData(task.adnAppId, appToken, startDate, endDate, 1, err);
            if (StringUtils.isNotBlank(jsonData) && err.length() == 0) {
                JSONObject jsonObject = JSONObject.parseObject(jsonData);
                int code = jsonObject.getIntValue("code");
                if (code == 200) {
                    int totalCnt = jsonObject.getIntValue("total_count");
                    int pageCount = BigDecimal.valueOf(totalCnt)
                            .divide(BigDecimal.valueOf(PAGE_SIZE), 0, RoundingMode.HALF_UP).intValue();
                    String data = jsonObject.getString("data");
                    err.append(jsonDataImportDatabase(task, data, startDate, 0));
                    if (pageCount > 1) {
                        for (int i = 2; i <= pageCount; i++) {
                            jsonData = downloadPageJsonData(task.adnAppId, appToken, startDate, endDate, i, err);
                            if (StringUtils.isNotBlank(jsonData) && err.length() == 0) {
                                JSONObject sObj = JSONObject.parseObject(jsonData);
                                int pageCode = sObj.getIntValue("code");
                                if (pageCode == 200) {
                                    err.append(jsonDataImportDatabase(task, sObj.getJSONObject("data").getString("lists"), startDate, i));
                                }
                            }
                        }
                    }
                    if (err.length() > 0) {
                        error = err.toString();
                    }
                } else {
                    error = "response code is not ok, res:" + jsonData;
                }
            } else {
                error = "response json data is null";
            }
        } catch (Exception e) {
            error = String.format("downJsonData error,msg:%s", e.getMessage());
        }
        return error;
    }

    private String downloadPageJsonData(String appId, String reportKey, String startDate, String endDate, int page, StringBuilder err) {
        String json_data = "";
        HttpEntity entity = null;
        LOG.info("[SharEit] downloadPageJsonData start, appId:{}, reportKey:{}, day:{}, page:{}", appId, reportKey, startDate, page);
        long start = System.currentTimeMillis();
        try {
            String params = buildSharEitParams(appId, reportKey, page, startDate, endDate);
            String url = String.format("http://report-api.hellay.net/report?%s", params);
            LOG.info("[SharEit] request url:{}", url);
            System.out.println(url);
            HttpGet httpGet = new HttpGet(url);
            RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(5 * 60 * 1000)
                    .setProxy(cfg.httpProxy).build();//设置请求和传输超时时间
            httpGet.setConfig(requestConfig);
            //发送Post,并返回一个HttpResponse对象
            HttpResponse response = MyHttpClient.getInstance().execute(httpGet);
            StatusLine sl = response.getStatusLine();
            if (sl.getStatusCode() != 200) {//如果状态码为200,就是正常返回
                err.append(String.format("request report response statusCode:%d", sl.getStatusCode()));
                if (response.getEntity() != null) {
                    err.append(EntityUtils.toString(response.getEntity()));
                }
                return json_data;
            }
            entity = response.getEntity();
            if (entity == null) {
                err.append("request report response enity is null");
                return json_data;
            }
            json_data = EntityUtils.toString(entity);
            return json_data;
        } catch (Exception e) {
            err.append(String.format("downJsonData error, msg:%s", e.getMessage()));
        } finally {
            EntityUtils.consumeQuietly(entity);
        }
        LOG.info("[SharEit] downloadPageJsonData end, appId:{}, reportKey:{}, day:{}, page:{} cost:{}", appId, reportKey, startDate, page, System.currentTimeMillis() - start);
        return json_data;
    }

    private String buildSharEitParams(String appId, String reportKey, int page, String start, String end) throws UnsupportedEncodingException {
        ParamsBuilder pb = new ParamsBuilder()
                .p("v", "1.0")
                .p("page_count", PAGE_SIZE)
                .p("app_id", appId)
                .p("page",page)
                .p("start_time", start)
                .p("end_time", end)
                .p("timestamp", System.currentTimeMillis())
                .p("report_key", reportKey);
        pb.params().sort(Comparator.comparing(NameValuePair::getName));
        String str = URLEncoder.encode(pb.format(), "UTF-8");
        String sign = Encrypter.md5(Encrypter.md5(Encrypter.md5(str)));
        pb.p("token", sign);
        return pb.format();
    }

    private String jsonDataImportDatabase(ReportTask task, String jsonData, String day, int page) {
        task.step = 2;
        if (page == 0) {
            String delSql = "delete from report_shareit where day=? and report_account_id=? ";
            try {
                jdbcTemplateW.update(delSql, day, task.reportAccountId);
            } catch (Exception e) {
                return String.format("delete report_SharEit error,msg:%s", e.getMessage());
            }
        }
        LOG.info("[SharEit] jsonDataImportDatabase start, taskId:{}, day:{}, page:{}", task.id, day, page);
        long start = System.currentTimeMillis();
        String error = "";

        try {
            String insertSql = "INSERT into report_shareit(day, country, placement, request, fill, fill_rate, impression, click, revenue, ecpm, ctr, report_account_id) " +
                    " VALUES(?,?,?,?,?,?,?,?,?,?,?,?)";

            List<Object[]> lsParm = new ArrayList<>(1000);
            JSONArray jsonArray = JSONArray.parseArray(jsonData);
            for (int i = 0; i < jsonArray.size(); i++) {
                JSONObject obj = jsonArray.getJSONObject(i);
                int request = obj.getIntValue("request");
                BigDecimal fillRate = obj.getBigDecimal("fill_rate");
                int fill = BigDecimal.valueOf(request).multiply(fillRate).intValue();
                Object[] params = new Object[]{day, obj.get("geo_code"), obj.get("placement"), request, fill,
                        fillRate, obj.get("impression"), obj.get("click"), obj.get("revenue"), obj.get("ecpm"),
                        obj.get("ctr"), task.reportAccountId};
                lsParm.add(params);
                if (lsParm.size() == 1000) {
                    jdbcTemplateW.batchUpdate(insertSql, lsParm);
                    lsParm.clear();
                }
            }
            if (!lsParm.isEmpty()) {
                jdbcTemplateW.batchUpdate(insertSql, lsParm);
            }
        } catch (Exception e) {
            error = String.format("insert report_SharEit error, msg:%s", e.getMessage());
        }
        LOG.info("[SharEit] jsonDataImportDatabase end, taskId:{}, day:{}, page:{}, cost:{}", task.id, day, page, System.currentTimeMillis() - start);
        return error;
    }

    private String savePrepareReportData(ReportTask task, String reportDay) {
        LOG.info("[SharEit] savePrepareReportData start, taskId:{}", task.id);
        long start = System.currentTimeMillis();
        String error;
        try {
            String dataSql = String.format("select day,country,concat('%s','_', placement) data_key,sum(request) AS api_request,sum(fill) AS api_filled," +
                    "sum(impression) AS api_impr,sum(click) AS api_click,sum(revenue) AS revenue " +
                    " from report_shareit where day=? and report_account_id=?" +
                    " group by day,country,placement ", task.adnAppId);

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
                String ak = MapHelper.getString(ins, "adn_app_key");
                String pk = MapHelper.getString(ins, "placement_key");
                if (StringUtils.isBlank(ak) || StringUtils.isBlank(pk)) {
                    continue;
                }
                String key = ak + "_" + pk;
                putLinkKeyMap(placements, key, ins, dataDay);
            }
            if (placements.isEmpty()) {
                return "The app key or placement key is not configured";
            }

            error = toAdnetworkLinked(task, task.adnAppToken, placements, data);
        } catch (Exception e) {
            error = String.format("savePrepareReportData error, msg:%s", e.getMessage());
        }
        LOG.info("[SharEit] savePrepareReportData end, taskId:{}, day:{}, cost:{}", task.id, reportDay, System.currentTimeMillis() - start);
        return error;
    }
}
