/*-------------------------------------------------------------------------
 *
 * shard_utils.c
 *
 * This file contains functions to perform useful operations on shards.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/namespace.h"
#include "utils/lsyscache.h"
#include "distributed/listutils.h"
#include "distributed/metadata_utility.h"
#include "distributed/metadata_cache.h"
#include "distributed/relay_utility.h"
#include "distributed/shard_utils.h"

/*
 * GetTableLocalShardOid returns the oid of the shard from the given distributed
 * relation with the shardId.
 */
Oid
GetTableLocalShardOid(Oid citusTableOid, uint64 shardId)
{
	const char *citusTableName = get_rel_name(citusTableOid);

	Assert(citusTableName != NULL);

	/* construct shard relation name */
	char *shardRelationName = pstrdup(citusTableName);
	AppendShardIdToName(&shardRelationName, shardId);

	Oid citusTableSchemaOid = get_rel_namespace(citusTableOid);

	Oid shardRelationOid = get_relname_relid(shardRelationName, citusTableSchemaOid);

	return shardRelationOid;
}


/*
 * CreateTableLocalShardVacuumRelations creates a list where there is one VacuumRelation
 * for each local shard placement for the given VacuumRelation(which belongs to shell table).
 */
List *
CreateTableLocalShardVacuumRelations(VacuumRelation *vacuumRel)
{
	bool missingOk = false;
	Oid relationId = RangeVarGetRelid(vacuumRel->relation, NoLock, missingOk);
	if (!IsCitusTable(relationId))
	{
		return NIL;
	}

	List *localShardPlacements = GroupShardPlacementsForTableOnGroup(relationId,
																	 GetLocalGroupId());

	List *vacuumShardRels = NIL;
	GroupShardPlacement *groupShardPlacement = NULL;

	char *relationName = vacuumRel->relation->relname;
	foreach_ptr(groupShardPlacement, localShardPlacements)
	{
		VacuumRelation *vacuumShardRel = copyObject(vacuumRel);

		char *shardName = pstrdup(relationName);
		AppendShardIdToName(&shardName, groupShardPlacement->shardId);
		vacuumShardRel->relation->relname = shardName;

		vacuumShardRels = lappend(vacuumShardRels, vacuumShardRel);
	}

	return vacuumShardRels;
}
