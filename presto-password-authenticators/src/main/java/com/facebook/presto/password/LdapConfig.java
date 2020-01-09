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

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;

import java.util.concurrent.TimeUnit;

public class LdapConfig
{
    private String ldapUrl;
    private String bindUserDN;
    private String bindPassword;
    private String userLoginAttribute;
    private String userAttributeSearchFilter;
    private String userBindSearchPattern;
    private String groupAuthorizationSearchPattern;
    private String userBaseDistinguishedName;
    private Duration ldapCacheTtl = new Duration(1, TimeUnit.HOURS);

    @NotNull
    @Pattern(regexp = "^ldaps://.*", message = "LDAP without SSL/TLS unsupported. Expected ldaps://")
    public String getLdapUrl()
    {
        return ldapUrl;
    }

    public String getBindUserDN()
    {
        return bindUserDN;
    }

    @Config("ldap.bind-user-dn")
    @ConfigDescription("Bind User of LDAP Server")
    public LdapConfig setBindUserDN(String bindUserDN)
    {
        this.bindUserDN = bindUserDN;
        return this;
    }

    @Config("ldap.bind-password")
    @ConfigDescription("Bind Password of LDAP Server")
    public LdapConfig setBindPassword(String bindPassword)
    {
        this.bindPassword = bindPassword;
        return this;
    }

    public String getBindPassword()
    {
        return bindPassword;
    }

    @Config("ldap.url")
    @ConfigDescription("URL of the LDAP server")
    public LdapConfig setLdapUrl(String url)
    {
        this.ldapUrl = url;
        return this;
    }

    @NotNull
    public String getUserBindSearchPattern()
    {
        return userBindSearchPattern;
    }

    @Config("ldap.user-bind-pattern")
    @ConfigDescription("Custom user bind pattern. Example: ${USER}@example.com")
    public LdapConfig setUserBindSearchPattern(String userBindSearchPattern)
    {
        this.userBindSearchPattern = userBindSearchPattern;
        return this;
    }

    public String getGroupAuthorizationSearchPattern()
    {
        return groupAuthorizationSearchPattern;
    }

    @Config("ldap.group-auth-pattern")
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

    @Config("ldap.user-base-dn")
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

    @Config("ldap.cache-ttl")
    public LdapConfig setLdapCacheTtl(Duration ldapCacheTtl)
    {
        this.ldapCacheTtl = ldapCacheTtl;
        return this;
    }

    public String getUserLoginAttribute()
    {
        return userLoginAttribute;
    }

    @Config("ldap.user-login-attribute")
    @ConfigDescription("Ldap search will return this user's attribute as a result of the auth process. Example: userPrincipalName")
    public LdapConfig setUserLoginAttribute(String userLoginAttribute)
    {
        this.userLoginAttribute = userLoginAttribute;
        return this;
    }

    public String getUserAttributeSearchFilter()
    {
        return userAttributeSearchFilter;
    }

    @Config("ldap.user-attribute-search-filter")
    @ConfigDescription("Ldap attribute to validate the user-bind-pattern against. Example: sAMAccountName")
    public LdapConfig setUserAttributeSearchFilter(String userAttributeSearchFilter)
    {
        this.userAttributeSearchFilter = userAttributeSearchFilter;
        return this;
    }
}
