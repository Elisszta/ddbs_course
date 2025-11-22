#!/bin/bash
# Setup MySQL master-slave replication configuration script

echo "=========================================="
echo "Setup MySQL Master-Slave Replication"
echo "=========================================="

# Wait for MySQL containers to be ready
echo "Waiting for MySQL containers to start..."
sleep 5

# Get master container IP (in Docker network)
MASTER_IP=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' mysql-master)

if [ -z "$MASTER_IP" ]; then
    echo "Error: Cannot get master IP address"
    exit 1
fi

echo "Master IP: $MASTER_IP"

# 1. Create replication user on master
echo ""
echo "1. Creating replication user on master..."
docker exec mysql-master mysql -uroot -proot <<EOF
CREATE USER IF NOT EXISTS 'repl'@'%' IDENTIFIED BY 'repl123';
GRANT REPLICATION SLAVE ON *.* TO 'repl'@'%';
FLUSH PRIVILEGES;
SELECT User, Host FROM mysql.user WHERE User='repl';
EOF

# 2. Get master GTID position
echo ""
echo "2. Getting master GTID position..."
MASTER_GTID=$(docker exec mysql-master mysql -uroot -proot -e "SELECT @@GLOBAL.GTID_EXECUTED;" -s -N 2>/dev/null | tail -1)
echo "Master GTID: $MASTER_GTID"

# 3. Configure slave1 to connect to master
echo ""
echo "3. Configuring slave1 (mysql-slave1)..."
docker exec mysql-slave1 mysql -uroot -proot <<EOF
STOP SLAVE;
RESET SLAVE;
CHANGE MASTER TO
    MASTER_HOST='$MASTER_IP',
    MASTER_PORT=3306,
    MASTER_USER='repl',
    MASTER_PASSWORD='repl123',
    MASTER_AUTO_POSITION=1;
START SLAVE;
EOF

# 4. Configure slave2 to connect to master
echo ""
echo "4. Configuring slave2 (mysql-slave2)..."
docker exec mysql-slave2 mysql -uroot -proot <<EOF
STOP SLAVE;
RESET SLAVE;
CHANGE MASTER TO
    MASTER_HOST='$MASTER_IP',
    MASTER_PORT=3306,
    MASTER_USER='repl',
    MASTER_PASSWORD='repl123',
    MASTER_AUTO_POSITION=1;
START SLAVE;
EOF

# 5. Check replication status
echo ""
echo "=========================================="
echo "Checking replication status"
echo "=========================================="

echo ""
echo "Slave1 (mysql-slave1) status:"
docker exec mysql-slave1 mysql -uroot -proot -e "SHOW SLAVE STATUS\G" 2>&1 | grep -E "Slave_IO_Running|Slave_SQL_Running|Seconds_Behind_Master|Last_IO_Error|Last_SQL_Error" | grep -v "Warning"

echo ""
echo "Slave2 (mysql-slave2) status:"
docker exec mysql-slave2 mysql -uroot -proot -e "SHOW SLAVE STATUS\G" 2>&1 | grep -E "Slave_IO_Running|Slave_SQL_Running|Seconds_Behind_Master|Last_IO_Error|Last_SQL_Error" | grep -v "Warning"

echo ""
echo "=========================================="
echo "Master-slave replication setup completed!"
echo "=========================================="
echo ""
echo "Verification steps:"
echo "1. On master: docker exec mysql-master mysql -uroot -proot -e \"CREATE DATABASE test_replication;\""
echo "2. Check slave: docker exec mysql-slave1 mysql -uroot -proot -e \"SHOW DATABASES LIKE 'test_replication';\""
echo "3. If you can see test_replication database, replication is working!"

