#!/bin/bash
# Setup MySQL master-slave replication (Bulletproof Version)
# Fixes: Uses single-line commands (-e) instead of heredoc to avoid formatting issues

echo "=========================================="
echo "Setup MySQL Master-Slave Replication"
echo "=========================================="

# Function to check MySQL readiness
wait_for_mysql() {
    local host=$1
    echo "Waiting for $host to be ready..."
    until docker exec $host mysql -uroot -proot -e "SELECT 1" > /dev/null 2>&1; do
        echo "   ...waiting for $host..."
        sleep 2
    done
    echo "$host is ready!"
}

# 1. Wait for all containers
wait_for_mysql "mysql-master"
wait_for_mysql "mysql-slave1"
wait_for_mysql "mysql-slave2"

# 2. Configure Master (Create Replication User)
echo ""
echo "🛠️  Creating replication user on Master..."
docker exec mysql-master mysql -uroot -proot -e "
    CREATE USER IF NOT EXISTS 'repl'@'%' IDENTIFIED WITH mysql_native_password BY 'repl123';
    GRANT REPLICATION SLAVE ON *.* TO 'repl'@'%';
    FLUSH PRIVILEGES;
" && echo "   -> User 'repl' created successfully."

# 3. Configure Slave 1
echo ""
echo "🛠️  Configuring Slave 1..."
docker exec mysql-slave1 mysql -uroot -proot -e "
    STOP SLAVE;
    RESET SLAVE ALL;
    CHANGE MASTER TO 
      MASTER_HOST='mysql-master', 
      MASTER_PORT=3306, 
      MASTER_USER='repl', 
      MASTER_PASSWORD='repl123', 
      MASTER_AUTO_POSITION=1;
    START SLAVE;
" && echo "   -> Slave 1 configured."

# 4. Configure Slave 2
echo ""
echo "Configuring Slave 2..."
docker exec mysql-slave2 mysql -uroot -proot -e "
    STOP SLAVE;
    RESET SLAVE ALL;
    CHANGE MASTER TO 
      MASTER_HOST='mysql-master', 
      MASTER_PORT=3306, 
      MASTER_USER='repl', 
      MASTER_PASSWORD='repl123', 
      MASTER_AUTO_POSITION=1;
    START SLAVE;
" && echo "   -> Slave 2 configured."

# 5. Final Status Check
echo ""
echo "=========================================="
echo "Checking Replication Status"
echo "=========================================="
sleep 2

check_status() {
    local host=$1
    echo ""
    echo "$host Status:"
    docker exec $host mysql -uroot -proot -e "SHOW SLAVE STATUS\G" | grep -E "Slave_IO_Running:|Slave_SQL_Running:|Last_IO_Error:|Last_SQL_Error:"
}

check_status "mysql-slave1"
check_status "mysql-slave2"

echo ""
echo "=========================================="
echo "Setup Completed!"
echo "=========================================="