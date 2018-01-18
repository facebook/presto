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
package com.facebook.presto.server.security;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import javax.validation.constraints.Size;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class LdapConfig
{
    private String ldapUrl;
    private List<String> userBindSearchPatterns = ImmutableList.of();
    private String groupAuthorizationSearchPattern;
    private String userBaseDistinguishedName;
    private Duration ldapCacheTtl = new Duration(1, TimeUnit.HOURS);

    @NotNull
    @Pattern(regexp = "^ldaps://.*", message = "LDAP without SSL/TLS unsupported. Expected ldaps://")
    public String getLdapUrl()
    {
        return ldapUrl;
    }

    @Config("authentication.ldap.url")
    @ConfigDescription("URL of the LDAP server")
    public LdapConfig setLdapUrl(String url)
    {
        this.ldapUrl = url;
        return this;
    }

    @NotNull
    @Size(min = 1)
    public List<String> getUserBindSearchPatterns()
    {
        return userBindSearchPatterns;
    }

    @Config("authentication.ldap.user-bind-pattern")
    @ConfigDescription("Custom user bind patterns. Example: ${USER}@example.com. You may specify multiple bind patterns separated by a semicolon.")
    public LdapConfig setUserBindSearchPatterns(String bindPatterns)
    {
        this.userBindSearchPatterns = ImmutableList.copyOf(Splitter.on(";").trimResults().omitEmptyStrings().split(bindPatterns));
        return this;
    }

    public String getGroupAuthorizationSearchPattern()
    {
        return groupAuthorizationSearchPattern;
    }

    @Config("authentication.ldap.group-auth-pattern")
    @ConfigDescription("Custom group authorization check query. Example: &(objectClass=user)(memberOf=cn=group)(user=username)")
    public LdapConfig setGroupAuthorizationSearchPattern(String groupAuthorizationSearchPattern)
    {
        this.groupAuthorizationSearchPattern = groupAuthorizationSearchPattern;
        return this;
    }

    public String getUserBaseDistinguishedName()
    {
        return userBaseDistinguishedName;
    }

    @Config("authentication.ldap.user-base-dn")
    @ConfigDescription("Base distinguished name of the user. Example: dc=example,dc=com")
    public LdapConfig setUserBaseDistinguishedName(String userBaseDistinguishedName)
    {
        this.userBaseDistinguishedName = userBaseDistinguishedName;
        return this;
    }

    @NotNull
    public Duration getLdapCacheTtl()
    {
        return ldapCacheTtl;
    }

    @Config("authentication.ldap.cache-ttl")
    public LdapConfig setLdapCacheTtl(Duration ldapCacheTtl)
    {
        this.ldapCacheTtl = ldapCacheTtl;
        return this;
    }
}
