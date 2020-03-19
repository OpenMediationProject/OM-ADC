// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.adc.web;

import com.adtiming.om.adc.dto.Response;
import com.adtiming.om.adc.util.JsonHelper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;

public class BaseController {

    private static final Logger LOG = LogManager.getLogger();

    @ExceptionHandler(Throwable.class)
    public void exceptionHandler(HttpServletRequest req, HttpServletResponse resp, Throwable t) throws IOException {
        resp.setCharacterEncoding("UTF-8");
        resp.setContentType("application/json; charset=UTF-8");

        boolean is_html = req.getRequestURI().endsWith(".html");
        if (t instanceof MissingServletRequestParameterException) {
            if (is_html) {
                resp.sendError(400, t.getMessage());
                return;
            }
            t = new Response(400, "missing_param", t.getMessage());
        }

        if (!(t instanceof Response)) {
            Throwable tmp = t;
            for (Throwable tr; (tr = tmp.getCause()) != null; ) {
                tmp = tr;
                if (tr instanceof Response) {
                    t = tr;
                    break;
                }
            }
            if (!(t instanceof Response)) {
                LOG.error("invoke method error: ", t);
                t = new Response(500, "unknown", "Server Error");
            }
        }
        try (PrintWriter writer = resp.getWriter()) {
            JsonHelper.getObjectMapper().writeValue(writer, t);
        } catch (Exception e) {
            LOG.error(e, e);
        }
    }

}
