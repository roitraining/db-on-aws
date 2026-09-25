-- Lab 1 completed: Orientation and Your First Query
-- Run block by block in the SQL editor.

-- set the session defaults (the extra level vs two-part names is the catalog)
USE CATALOG training_nic;
USE SCHEMA migrated;

-- first look at the migrated table
SELECT * FROM institutions LIMIT 100;

-- the cloud row count: 4,900 against the on-premises 5,000 -- the gap is Lab 3's job
SELECT COUNT(*) AS row_count FROM institutions;
