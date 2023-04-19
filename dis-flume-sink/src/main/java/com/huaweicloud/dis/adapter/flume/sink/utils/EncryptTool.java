package com.huaweicloud.dis.adapter.flume.sink.utils;

import com.huaweicloud.dis.util.encrypt.EncryptUtils;

public class EncryptTool
{
    private EncryptTool()
    {
    }

    public static String encrypt(String data, String key)
    {
        try
        {
            if (key == null)
            {
                throw new RuntimeException("Encrypt key is null.");
            }
            return EncryptUtils.gen(new String[]{key}, data);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public static String decrypt(String data, String key)
    {
        try
        {
            if (key == null)
            {
                throw new RuntimeException("Encrypt key is null.");
            }
            return EncryptUtils.dec(new String[]{key}, data);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args)
    {
        if (args.length < 2)
        {
            doUsage();
            System.exit(-1);
        }
        System.out.println(encrypt(args[0], args[1]));
    }

    private static void doUsage()
    {
        System.out.println("Please input password and key.");
    }
}
