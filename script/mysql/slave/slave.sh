#!/bin/bash
#定义连接master进行同步的账号
SLAVE_SYNC_USER="${SLAVE_SYNC_USER:-sync_admin}"
#定义连接master进行同步的账号密码
SLAVE_SYNC_PASSWORD="${SLAVE_SYNC_PASSWORD:-123456}"
#定义slave数据库账号
ADMIN_USER="${ADMIN_USER:-root}"
#定义slave数据库密码
ADMIN_PASSWORD="${ADMIN_PASSWORD:-root}"
#定义连接master数据库host地址
MASTER_HOST="${MASTER_HOST:-%}"
#连接master数据库，查询二进制数据，并解析出logfile和pos，这里同步用户要开启 REPLICATION CLIENT权限，才能使用SHOW MASTER STATUS;
RESULT=`mysql -u"$SLAVE_SYNC_USER" -h$MASTER_HOST -p"$SLAVE_SYNC_PASSWORD" -e "SHOW MASTER STATUS;" | grep -v grep |tail -n +2| awk '{print $1,$2}'`
#解析出logfile
LOG_FILE_NAME=`echo $RESULT | grep -v grep | awk '{print $1}'`
#解析出pos
LOG_FILE_POS=`echo $RESULT | grep -v grep | awk '{print $2}'`
#设置连接master的同步相关信息
SYNC_SQL="change master to master_host='$MASTER_HOST',master_user='$SLAVE_SYNC_USER',master_password='$SLAVE_SYNC_PASSWORD',master_log_file='$LOG_FILE_NAME',master_log_pos=$LOG_FILE_POS,get_master_public_key=1;"
#开启同步
START_SYNC_SQL="start slave;"
#查看同步状态
STATUS_SQL="show slave status\G;"
mysql -u"$ADMIN_USER" -p"$ADMIN_PASSWORD" -e "$SYNC_SQL $START_SYNC_SQL $STATUS_SQL"
