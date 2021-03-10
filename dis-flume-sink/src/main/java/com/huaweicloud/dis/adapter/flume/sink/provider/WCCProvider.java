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

package com.huaweicloud.dis.adapter.flume.sink.provider;

import com.huaweicloud.dis.adapter.flume.sink.WCCTool;
import com.huaweicloud.dis.core.DISCredentials;
import com.huaweicloud.dis.core.util.StringUtils;
import com.huaweicloud.dis.util.config.ICredentialsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WCCProvider implements ICredentialsProvider
{
    private static final Logger LOG = LoggerFactory.getLogger(WCCProvider.class);
    
    private boolean decrypted = false;
    
    private DISCredentials decryptedCredentials;
    
    @Override
    public DISCredentials updateCredentials(DISCredentials disCredentials)
    {
        if (!decrypted)
        {
            synchronized (WCCProvider.class)
            {
                if (!decrypted)
                {
                    String decryptAK = WCCTool.getInstance().decrypt(disCredentials.getAccessKeyId());
                    String decryptSK = WCCTool.getInstance().decrypt(disCredentials.getSecretKey());
                    
                    String securityToken = disCredentials.getSecurityToken();
                    if (!StringUtils.isNullOrEmpty(securityToken))
                    {
                        securityToken = WCCTool.getInstance().decrypt(securityToken);
                    }
                    
                    String dataPassword = disCredentials.getDataPassword();
                    if (!StringUtils.isNullOrEmpty(dataPassword))
                    {
                        dataPassword = WCCTool.getInstance().decrypt(dataPassword);
                    }
                    
                    decryptedCredentials = new DISCredentials(decryptAK, decryptSK, securityToken, Long.valueOf(dataPassword));
                    decrypted = true;
                    LOG.info("WCCProvider decrypted successfully.");
                    return decryptedCredentials;
                }
                else
                {
                    return decryptedCredentials;
                }
            }
        }
        return disCredentials;
    }
}
