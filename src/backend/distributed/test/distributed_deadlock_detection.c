/*-------------------------------------------------------------------------
 *
 * test/src/distributed_deadlock_detection.c
 *
 * This file contains functions to exercise distributed deadlock detection
 * related lower level functionality.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "distributed/distributed_deadlock_detection.h"
#include "distributed/metadata_cache.h"
#include "distributed/tuplestore.h"


PG_FUNCTION_INFO_V1(get_adjacency_list_wait_graph);


/*
 * get_adjacency_list_wait_graph returns the wait graph in adjacency list format. For the
 * details see BuildAdjacencyListForWaitGraph().
 *
 * This function is mostly useful for testing and debugging purposes.
 */
Datum
get_adjacency_list_wait_graph(PG_FUNCTION_ARGS)
{
	TupleDesc tupleDescriptor = NULL;

	HASH_SEQ_STATUS status;
	TransactionNode *transactionNode = NULL;

	Datum values[2];
	bool isNulls[2];

	CheckCitusVersion(ERROR);

	Tuplestorestate *tupleStore = SetupTuplestore(fcinfo, &tupleDescriptor);
	WaitGraph *waitGraph = BuildGlobalWaitGraph();
	HTAB *adjacencyList = BuildAdjacencyListsForWaitGraph(waitGraph);

	/* iterate on all nodes */
	hash_seq_init(&status, adjacencyList);

	while ((transactionNode = (TransactionNode *) hash_seq_search(&status)) != 0)
	{
		memset(values, 0, sizeof(values));
		memset(isNulls, false, sizeof(isNulls));

		values[0] = UInt64GetDatum(transactionNode->transactionId.transactionNumber);
		values[1] = CStringGetDatum(WaitsForToString(transactionNode->waitsFor));

		tuplestore_putvalues(tupleStore, tupleDescriptor, values, isNulls);
	}

	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupleStore);

	PG_RETURN_VOID();
}
