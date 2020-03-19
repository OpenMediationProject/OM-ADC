// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.adc.util;

import java.io.ByteArrayInputStream;
import java.nio.charset.Charset;

public class ByteArrayOutputStream extends java.io.ByteArrayOutputStream {

    public ByteArrayOutputStream() {
        super();
    }

    public ByteArrayOutputStream(int size) {
        super(size);
    }

    /**
     * becareful with this buf, it's managed by super class
     *
     * @return the super class's buf
     */
    public byte[] getBuf() {
        return buf;
    }

    public ByteArrayInputStream toInputStream() {
        return new ByteArrayInputStream(buf, 0, count);
    }

    /**
     * @param charset
     * @since 2.1.5
     */
    public String toString(Charset charset) {
        return new String(buf, 0, count, charset);
    }

}

