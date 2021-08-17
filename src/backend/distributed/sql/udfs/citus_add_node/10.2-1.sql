DROP FUNCTION pg_catalog.citus_add_node(text,int,int,noderole,name);

CREATE FUNCTION pg_catalog.citus_add_node(nodename text,
                                          nodeport integer,
                                          groupid integer default -1,
                                          noderole noderole default 'primary',
                                          nodecluster name default 'default' ,
                                          metadatasynced boolean default false)
  RETURNS INTEGER
  LANGUAGE C STRICT
  AS 'MODULE_PATHNAME', $$citus_add_node$$;
COMMENT ON FUNCTION pg_catalog.citus_add_node(nodename text, nodeport integer,
                                              groupid integer, noderole noderole,
                                              nodecluster name, metadatasynced boolean)
  IS 'add node to the cluster';
REVOKE ALL ON FUNCTION pg_catalog.citus_add_node(text,int,int,noderole,name,boolean) FROM PUBLIC;
