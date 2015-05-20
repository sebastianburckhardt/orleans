#ifndef __DIAGNOSTICS__
#define __DIAGNOSTICS__
#pragma once
#include <windef.h>

enum DiagnosticLogLevel
{
   LlUndefined = 0,
   LlCritical,
   LlError,
   LlWarning,
   LlInformation,
   LlVerbose,
};

STDAPI
DiagnosticsInitialize(__in LPCWSTR DiagnosticStorePath);

STDAPI
DiagnosticsWriteToLog(
    __in DiagnosticLogLevel level,
    __in LPCWSTR format, 
    ...);

// Collect dumps to the default directory
STDAPI
DiagnosticsCrashDumpsEnableCollection(
    __in BOOL collectFullDumps);

// Collect dumps to an explict directory.
STDAPI
DiagnosticsCrashDumpsEnableCollectionToDirectory(
    __in LPCWSTR crashDumpDirectory,
    __in BOOL collectFullDumps);

#endif
