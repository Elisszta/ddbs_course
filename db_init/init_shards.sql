-- 1. Main Campus Server (Global Master)
CREATE DATABASE IF NOT EXISTS global_master;

-- 2. Three Shard Databases for Courses
CREATE DATABASE IF NOT EXISTS shard_a; -- Main campus courses
CREATE DATABASE IF NOT EXISTS shard_b; -- Branch campus 1 courses
CREATE DATABASE IF NOT EXISTS shard_c; -- Branch campus 2 courses

-- 3. (Optional) Test table creation to verify connections
USE global_master;
CREATE TABLE IF NOT EXISTS connection_test (
    id INT PRIMARY KEY,
    msg VARCHAR(50)
);
INSERT INTO connection_test VALUES (1, 'Success: Global Master Connected');