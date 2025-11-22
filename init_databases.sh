#!/bin/bash
# Initialize databases using original master.sql and shard.sql
# Convert encoding (UTF-16 -> UTF-8) before import

echo "=========================================="
echo "Initialize databases with original SQL files"
echo "=========================================="

# Convert SQL file encoding (UTF-16 -> UTF-8)
echo ""
echo "0. Converting SQL file encoding (UTF-16 -> UTF-8)..."
iconv -f UTF-16LE -t UTF-8 db_init/master.sql > /tmp/master_utf8.sql 2>/dev/null || \
    iconv -f UTF-16 -t UTF-8 db_init/master.sql > /tmp/master_utf8.sql 2>/dev/null || \
    echo "Warning: Encoding conversion may fail, using original file"

iconv -f UTF-16LE -t UTF-8 db_init/shard.sql > /tmp/shard_utf8.sql 2>/dev/null || \
    iconv -f UTF-16 -t UTF-8 db_init/shard.sql > /tmp/shard_utf8.sql 2>/dev/null || \
    echo "Warning: Encoding conversion may fail, using original file"

# Drop and recreate database (clean initialization)
echo ""
echo "1. Drop and recreate database school (clean initialization)..."
echo "   Note: Master-slave replication will auto-sync, only operate on master"

# Master-slave: Only operate on master, slaves will auto-sync via replication
echo "   Drop and recreate master (slaves will auto-sync)..."
docker exec mysql-master mysql -uroot -proot -e "DROP DATABASE IF EXISTS school; CREATE DATABASE school;" 2>/dev/null
echo "   Waiting for replication sync (3 seconds)..."
sleep 3

# Shard: Independent operation (no replication)
echo "   Drop and recreate shard databases..."
for db_node in mysql-shard0 mysql-shard1 mysql-shard2; do
    docker exec $db_node mysql -uroot -proot -e "DROP DATABASE IF EXISTS school; CREATE DATABASE school;" 2>/dev/null
done

# Initialize master (skip GTID_PURGED line)
echo ""
echo "2. Initialize master (mysql-master)..."
echo "   Note: Table structure will auto-sync to slaves via replication"
if [ -f /tmp/master_utf8.sql ]; then
    sed '/SET @@GLOBAL.GTID_PURGED/d' /tmp/master_utf8.sql | docker exec -i mysql-master mysql -uroot -proot school 2>&1 | grep -v "Warning" | grep -v "^$" || echo "✓ Master initialized"
else
    sed '/SET @@GLOBAL.GTID_PURGED/d' db_init/master.sql | docker exec -i mysql-master mysql -uroot -proot school 2>&1 | grep -v "Warning" | grep -v "^$" || echo "✓ Master initialized"
fi

echo "   Waiting for replication sync (3 seconds)..."
sleep 3

# Initialize shard databases
echo ""
echo "3. Initialize shard databases..."
echo "   Initializing shard0 (mysql-shard0)..."
if [ -f /tmp/shard_utf8.sql ]; then
    cat /tmp/shard_utf8.sql | docker exec -i mysql-shard0 mysql -uroot -proot school 2>&1 | grep -v "Warning" | grep -v "^$" || echo "   ✓ Shard0 initialized"
else
    cat db_init/shard.sql | docker exec -i mysql-shard0 mysql -uroot -proot school 2>&1 | grep -v "Warning" | grep -v "^$" || echo "   ✓ Shard0 initialized"
fi

echo "   Initializing shard1 (mysql-shard1)..."
if [ -f /tmp/shard_utf8.sql ]; then
    cat /tmp/shard_utf8.sql | docker exec -i mysql-shard1 mysql -uroot -proot school 2>&1 | grep -v "Warning" | grep -v "^$" || echo "   ✓ Shard1 initialized"
else
    cat db_init/shard.sql | docker exec -i mysql-shard1 mysql -uroot -proot school 2>&1 | grep -v "Warning" | grep -v "^$" || echo "   ✓ Shard1 initialized"
fi

echo "   Initializing shard2 (mysql-shard2)..."
if [ -f /tmp/shard_utf8.sql ]; then
    cat /tmp/shard_utf8.sql | docker exec -i mysql-shard2 mysql -uroot -proot school 2>&1 | grep -v "Warning" | grep -v "^$" || echo "   ✓ Shard2 initialized"
else
    cat db_init/shard.sql | docker exec -i mysql-shard2 mysql -uroot -proot school 2>&1 | grep -v "Warning" | grep -v "^$" || echo "   ✓ Shard2 initialized"
fi

# Cleanup temporary files
rm -f /tmp/master_utf8.sql /tmp/shard_utf8.sql 2>/dev/null

echo ""
echo "=========================================="
echo "Database initialization completed!"
echo "=========================================="
echo ""
echo "Verifying table structure:"
echo "Master tables:"
docker exec mysql-master mysql -uroot -proot school -e "SHOW TABLES;" 2>&1 | grep -v "Warning" | grep -E "Tables_in_school|student|teacher|selection_batch"
echo ""
echo "Slave1 tables (replication sync):"
docker exec mysql-slave1 mysql -uroot -proot school -e "SHOW TABLES;" 2>&1 | grep -v "Warning" | grep -E "Tables_in_school|student|teacher|selection_batch"
echo ""
echo "Shard0 tables:"
docker exec mysql-shard0 mysql -uroot -proot school -e "SHOW TABLES;" 2>&1 | grep -v "Warning" | grep -E "Tables_in_school|course|learn|teach"
