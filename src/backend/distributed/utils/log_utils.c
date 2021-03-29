/*-------------------------------------------------------------------------
 *
 * log_utils.c
 *	  Utilities regarding logs
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "utils/guc.h"
#include "distributed/log_utils.h"


/*
 * HashLogMessage is only supported in Citus Enterprise
 */
char *
HashLogMessage(const char *logText)
{
	return (char *) logText;
}
