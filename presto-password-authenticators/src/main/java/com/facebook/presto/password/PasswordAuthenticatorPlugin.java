/*
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
package com.facebook.presto.password;

import com.facebook.presto.common.security.PasswordAuthenticatorFactory;
import com.facebook.presto.password.file.FileAuthenticatorFactory;
import com.facebook.presto.password.ldap.LdapAuthenticatorFactory;
import com.facebook.presto.spi.Plugin;
import com.google.common.collect.ImmutableList;

public class PasswordAuthenticatorPlugin
        implements Plugin
{
    @Override
    public Iterable<PasswordAuthenticatorFactory> getPasswordAuthenticatorFactories()
    {
        return ImmutableList.<PasswordAuthenticatorFactory>builder()
                .add(new LdapAuthenticatorFactory())
                .add(new FileAuthenticatorFactory())
                .build();
    }
}
