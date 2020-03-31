// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.adc.util;

import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;

public abstract class ParserHelper {

    private ParserHelper() {
    }

    public static Charset analysisCharset(HttpEntity entity) throws IOException {
        ContentType contentType = ContentType.get(entity);
        if (contentType != null) {
            return contentType.getCharset();
        }
        return StandardCharsets.UTF_8;
    }

    public static ByteArrayOutputStream getContentAsBytes(HttpEntity entity) throws IOException {
        InputStream in = entity.getContent();
        long length = entity.getContentLength();
        ByteArrayOutputStream out = new ByteArrayOutputStream(length < 0 ? 1024 : (int) length);
        IOUtils.copy(in, out);
        return out;
    }

    public static Reader getContentReader(HttpEntity entity, Charset defaultCharset) throws IOException {
        Charset charset = analysisCharset(entity);
        InputStream in = entity.getContent();
        Header contentEncoding = entity.getContentEncoding();
        if (contentEncoding != null && entity.getContentLength() != 0
                && "gzip".equalsIgnoreCase(contentEncoding.getValue())) {
            in = new GZIPInputStream(in);
        }
        return new InputStreamReader(in, charset == null ? defaultCharset : charset);
    }

    public static Reader getContentReader(HttpEntity entity) throws IOException {
        return getContentReader(entity, StandardCharsets.UTF_8);
    }

    public static String getContentAsString(HttpEntity entity) throws IOException {
        return IOUtils.toString(getContentReader(entity));
    }

    public static String getContentAsString(HttpEntity entity, Charset defaultCharset) throws IOException {
        return IOUtils.toString(getContentReader(entity, defaultCharset));
    }

}
