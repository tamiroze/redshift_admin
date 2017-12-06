--modify columns length in Redshift

ALTER TABLE TABLE_NAME ADD COLUMN column_name_new VARCHAR(4000) ENCODE lzo;
UPDATE TABLE_NAME SET column_name_new = column_name;
ALTER TABLE TABLE_NAME DROP COLUMN column_name;
ALTER TABLE TABLE_NAME RENAME COLUMN column_name_new TO column_name;
