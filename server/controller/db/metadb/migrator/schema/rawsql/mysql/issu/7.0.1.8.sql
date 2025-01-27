DROP PROCEDURE IF EXISTS AddIDPrimaryKey;

CREATE PROCEDURE AddIDPrimaryKey(
    IN tableName VARCHAR(255)
)
BEGIN
    DECLARE primary_key_count INT;
    DECLARE auto_increment_column VARCHAR(255);
    DECLARE column_exists INT;

    -- 检查表是否存在 PRIMARY KEY
    SELECT COUNT(*)
    INTO primary_key_count
    FROM information_schema.table_constraints
    WHERE table_schema = DATABASE()
      AND table_name = tableName
      AND constraint_type = 'PRIMARY KEY';

    -- 如果存在 PRIMARY KEY
    IF primary_key_count > 0 THEN
        -- 修改 id 列，移除 AUTO_INCREMENT，以便删除复合主键
        SET @sql = CONCAT('ALTER TABLE ', tableName, ' MODIFY id INTEGER NOT NULL');
        PREPARE stmt FROM @sql;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
        -- END IF;

        -- 删除现有的 PRIMARY KEY
        SET @sql = CONCAT('ALTER TABLE ', tableName, ' DROP PRIMARY KEY');
        PREPARE stmt FROM @sql;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
    END IF;

    -- 如果 id 列存在，则将其设为主键
    SET @sql = CONCAT('ALTER TABLE ', tableName, ' MODIFY id INTEGER NOT NULL AUTO_INCREMENT, ADD PRIMARY KEY (id)');
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END;

CALL AddIDPrimaryKey("vl2");
CALL AddIDPrimaryKey("vm");
CALL AddIDPrimaryKey("vinterface");
CALL AddIDPrimaryKey("ip_resource");
CALL AddIDPrimaryKey("floatingip");
CALL AddIDPrimaryKey("host_device");
CALL AddIDPrimaryKey("vnet");

DROP PROCEDURE AddIDPrimaryKey;

UPDATE db_version SET version='7.0.1.8';
