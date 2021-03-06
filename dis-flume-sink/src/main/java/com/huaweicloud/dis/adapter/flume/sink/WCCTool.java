/*
 * Copyright 2002-2010 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.huaweicloud.dis.adapter.flume.sink;

import org.wcc.crypt.CrypterFactory;

import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;

public class WCCTool
{
    private static final WCCTool WCC_TOOL = new WCCTool();
    
    private static final String DEFAULT_WCC_PATH = getDefaultWCCPath();
    
    private WCCTool()
    {
    }
    
    public static WCCTool getInstance()
    {
        return WCC_TOOL;
    }
    
    public static final String WCC_HOME_PATH_KEY = "beetle.application.home.path";
    
    public String encrypt(String data)
    {
        withDefaultWCCPath();
        return CrypterFactory.getCrypter(CrypterFactory.AES_CBC).encrypt(data);
    }
    
    public String decrypt(String data)
    {
        withDefaultWCCPath();
        return CrypterFactory.getCrypter(CrypterFactory.AES_CBC).decrypt(data);
    }
    
    public String getWCCPath()
    {
        return System.getProperty(WCC_HOME_PATH_KEY);
    }
    
    private static String getDefaultWCCPath()
    {
        URL url = WCCTool.class.getProtectionDomain().getCodeSource().getLocation();
        String filePath = null;
        try
        {
            filePath = URLDecoder.decode(url.getPath(), "utf-8");
        }
        catch (UnsupportedEncodingException e)
        {
            filePath = url.getPath();
        }
        filePath = filePath.substring(0, filePath.lastIndexOf("/") + 1);
        return filePath;
    }
    
    public WCCTool withDefaultWCCPath()
    {
        if (!isEmpty(getWCCPath()))
        {
            return WCC_TOOL;
        }
        
        return withWCCPath(DEFAULT_WCC_PATH);
    }
    
    public WCCTool withWCCPath(String wccPath)
    {
        if (!isEmpty(wccPath))
        {
            System.setProperty(WCC_HOME_PATH_KEY, wccPath);
        }
        else
        {
            System.clearProperty(WCC_HOME_PATH_KEY);
        }
        return WCC_TOOL;
    }
    
    public static void main(String[] args)
    {
        if (args.length != 1)
        {
            doUsage();
            System.exit(-1);
        }
        System.out.println(WCCTool.getInstance().encrypt(args[0]));
    }
    
    private static void doUsage()
    {
        System.out.println("Please input password.");
    }
    
    private boolean isEmpty(String str)
    {
        return str == null || "".equals(str);
    }
}
