/*-------------------------------------------------------------------------
 * log_utils.h
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef LOG_UTILS_H
#define LOG_UTILS_H


#include "utils/guc.h"

/* do not log */
#define CITUS_LOG_LEVEL_OFF 0


extern char * HashLogMessage(const char *text);

#define ApplyLogRedaction(text) \
	(log_min_messages <= ereport_loglevel ? HashLogMessage(text) : text)

/*
 * IsLoggableLevel evaluates to true if either of client or server log
 * guc is configured to log the given log level.
 * In postgres, log can be configured differently for clients and servers.
 */
#define IsLoggableLevel(logLevel) \
	(log_min_messages <= logLevel || client_min_messages <= logLevel)

#undef ereport
#define ereport(elevel, rest) \
	do { \
		int ereport_loglevel = elevel; \
		(void) (ereport_loglevel); \
		ereport_domain(elevel, TEXTDOMAIN, rest); \
	} while (0)

#endif /* LOG_UTILS_H */
