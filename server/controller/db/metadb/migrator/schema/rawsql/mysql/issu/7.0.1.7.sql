DROP PROCEDURE IF EXISTS AddIndexIfNotExists;

CREATE PROCEDURE AddIndexIfNotExists(
    IN tableName VARCHAR(255),
    IN indexName VARCHAR(255),
    IN indexCol VARCHAR(255)
)
BEGIN
    DECLARE index_count INT;

    -- check if index exists
    SELECT COUNT(*)
    INTO index_count
    FROM information_schema.statistics
    WHERE table_schema = DATABASE()
    AND table_name = tableName
    AND index_name = indexName;

    -- if index not exists, add index
    IF index_count = 0 THEN
        SET @sql = CONCAT('ALTER TABLE ', tableName, ' ADD INDEX ', indexName, ' (', indexCol, ') USING BTREE');
        PREPARE stmt FROM @sql;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
    END IF;
END;

CALL AddIndexIfNotExists('routing_table', 'vnet_id_index', 'vnet_id');
CALL AddIndexIfNotExists('vl2', 'region_index', 'region');
CALL AddIndexIfNotExists('vinterface', 'epc_id_index', 'epc_id');
CALL AddIndexIfNotExists('vm', 'launch_server_index', 'launch_server');
CALL AddIndexIfNotExists('vm', 'epc_id_index', 'epc_id');
CALL AddIndexIfNotExists('vm', 'az_index', 'az');
CALL AddIndexIfNotExists('vm', 'region_index', 'region');
CALL AddIndexIfNotExists('epc', 'region_index', 'region');
CALL AddIndexIfNotExists('nat_rule', 'nat_id_index', 'nat_id');
CALL AddIndexIfNotExists('lb_listener', 'lb_id_index', 'lb_id');
CALL AddIndexIfNotExists('lb_target_server', 'lb_id_index', 'lb_id');
CALL AddIndexIfNotExists('pod_node', 'pod_cluster_id_index', 'pod_cluster_id');
CALL AddIndexIfNotExists('pod_node', 'epc_id_index', 'epc_id');
CALL AddIndexIfNotExists('pod_node', 'az_index', 'az');
CALL AddIndexIfNotExists('pod_node', 'region_index', 'region');
CALL AddIndexIfNotExists('pod', 'state_index', 'state');
CALL AddIndexIfNotExists('pod', 'pod_rs_id_index', 'pod_rs_id');
CALL AddIndexIfNotExists('pod', 'pod_group_id_index', 'pod_group_id');
CALL AddIndexIfNotExists('pod', 'pod_node_id_index', 'pod_node_id');
CALL AddIndexIfNotExists('pod', 'pod_namespace_id_index', 'pod_namespace_id');
CALL AddIndexIfNotExists('pod', 'pod_cluster_id_index', 'pod_cluster_id');
CALL AddIndexIfNotExists('pod', 'epc_id_index', 'epc_id');
CALL AddIndexIfNotExists('pod', 'az_index', 'az');
CALL AddIndexIfNotExists('pod', 'region_index', 'region');
CALL AddIndexIfNotExists('pod_rs', 'domain_index', 'domain');
CALL AddIndexIfNotExists('pod_rs', 'pod_group_id_index', 'pod_group_id');
CALL AddIndexIfNotExists('pod_rs', 'pod_namespace_id_index', 'pod_namespace_id');
CALL AddIndexIfNotExists('pod_group', 'pod_namespace_id_index', 'pod_namespace_id');
CALL AddIndexIfNotExists('pod_group', 'pod_cluster_id_index', 'pod_cluster_id');
CALL AddIndexIfNotExists('pod_service', 'pod_ingress_id_index', 'pod_ingress_id');
CALL AddIndexIfNotExists('pod_service', 'pod_namespace_id_index', 'pod_namespace_id');
CALL AddIndexIfNotExists('pod_service', 'pod_cluster_id_index', 'pod_cluster_id');
CALL AddIndexIfNotExists('pod_service_port', 'pod_service_id_index', 'pod_service_id');
CALL AddIndexIfNotExists('pod_ingress_rule', 'pod_ingress_id_index', 'pod_ingress_id');

DROP PROCEDURE AddIndexIfNotExists;

UPDATE db_version SET version='7.0.1.7';
