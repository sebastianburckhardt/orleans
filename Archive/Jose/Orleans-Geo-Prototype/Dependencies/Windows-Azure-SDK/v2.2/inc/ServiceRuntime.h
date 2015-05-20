#ifndef __SERVICE_RUNTIME__
#define __SERVICE_RUNTIME__
#pragma once
#include <windef.h>
#define ROLE_ENVIRONMENT_E_INSUFFICIENT_BUFFER HRESULT_FROM_WIN32(ERROR_INSUFFICIENT_BUFFER)

STDAPI
RoleEnvironmentInitialize(void);

STDAPI
RoleEnvironmentGetConfigurationSettingValueW(
    __in LPCWSTR name,
    __out_ecount(cchDest) LPWSTR pszDest,
    __in size_t cchDest,
    __out_opt size_t *pcchRequiredDestSize);

STDAPI
RoleEnvironmentGetConfigurationSettingValueA(
    __in LPCSTR name,
    __out_ecount(cchDest) LPSTR pszDest,
    __in size_t cchDest,
    __out_opt size_t *pcchRequiredDestSize);

typedef struct _LOCALRESOURCE* LPLOCALRESOURCE;

STDAPI
RoleEnvironmentGetLocalResourceW(
    __in LPCWSTR name,
    __deref_out LPLOCALRESOURCE* ppout);

STDAPI
RoleEnvironmentGetLocalResourceA(
    __in LPCSTR name,
    __deref_out LPLOCALRESOURCE* ppout);

STDAPI
LocalResourceGetMaximumSizeInMegabytes(
    __in  LPLOCALRESOURCE plrc,
    __out PULONG pdw);

STDAPI
LocalResourceGetNameW(
    __in LPLOCALRESOURCE plrc,
    __out_ecount(cchDest) LPWSTR pszDest,
    __in size_t cchDest,
    __out_opt size_t *pcchRequiredDestSize);

STDAPI
LocalResourceGetNameA(
    __in LPLOCALRESOURCE plrc,
    __out_ecount(cchDest) LPSTR pszDest,
    __in size_t cchDest,
    __out_opt size_t *pcchRequiredDestSize);

STDAPI
LocalResourceGetRootPathW(
    __in LPLOCALRESOURCE plrc,
    __out_ecount(cchDest) LPWSTR pszDest,
    __in size_t cchDest,
    __out_opt size_t *pcchRequiredDestSize);

STDAPI
LocalResourceGetRootPathA(
    __in LPLOCALRESOURCE plrc,
    __out_ecount(cchDest) LPSTR pszDest,
    __in size_t cchDest,
    __out_opt size_t *pcchRequiredDestSize);

#ifdef UNICODE
#define RoleEnvironmentGetConfigurationSettingValue  RoleEnvironmentGetConfigurationSettingValueW
#define RoleEnvironmentGetLocalResource  RoleEnvironmentGetLocalResourceW
#define LocalResourceGetName LocalResourceGetNameW
#define LocalResourceGetRootPath  LocalResourceGetRootPathW
#else
#define RoleEnvironmentGetConfigurationSettingValue  RoleEnvironmentGetConfigurationSettingValueA
#define RoleEnvironmentGetLocalResource  RoleEnvironmentGetLocalResourceA
#define LocalResourceGetName LocalResourceGetNameA
#define LocalResourceGetRootPath  LocalResourceGetRootPathA
#endif /* !UNICODE */
#endif