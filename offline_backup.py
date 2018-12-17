import os
import os.path
import datetime
import time
from datetime import datetime
import MySQLdb
from boto.ec2 import connect_to_region
import kayvee
import logging
import fcntl
import yaml

try:
    import boto3
except (ImportError, ValueError):
    print ("pip install boto3")
    pass

ec2reource = boto3.resource('ec2', region_name='us-east-1')
ec = boto3.client('ec2', region_name='us-east-1')
timesec = datetime.now()

"""
    Goal :
      To be able to do a cold database backup on a single database instance.
    Process :
      stop mysql server
      execute backup
      start mysql server
    Assumptions
      To stop the server  - Server must be running
                          - Server must not have more than 3 hours lag
                          - Not snapshot should be running
                          - Last backup must be at least 24h old
                            TBD

      To execute backup   - Server must be stopped
      To start the server - Server must be stopped
                          - Snapshot must be completed
                          - Lock file must exist
    
    How to:
        Te process will run in a automatic process every N minutes. 
"""

MYSQL_CONFIG = {
    'Host': 'localhost',
    'Port': 3306,
    'User': 'monitor',
    'Password': 'monitor',
    'Db': '',
}

INSTANCE_CONFIG = {
    'InstanceId': '',
    'VolumeId': '',
    'InstanceName': '',
    'LockFilename': '',
    'TagRole': '',
    'TagSchema': '',
    'Environment': '',
    'Retention': '',
}


def set_mysql_config():
    """
    Read config.yml and setting variables
    """
    try:
        stream = open("config.yml", "r")
        docs = yaml.load_all(stream)
        for doc in docs:
            MYSQL_CONFIG["Host"] = doc['mysql']["host"]
            MYSQL_CONFIG["Port"] = doc['mysql']["port"]
            MYSQL_CONFIG["User"] = doc['mysql']["user"]
            MYSQL_CONFIG["Password"] = doc['mysql']["password"]
            MYSQL_CONFIG["Db"] = doc['mysql']["db"]

            INSTANCE_CONFIG["InstanceId"] = doc['instance']["instance_id"]
            INSTANCE_CONFIG["VolumeId"] = doc['instance']["volume_id"]
            INSTANCE_CONFIG["InstanceName"] = doc['instance']["g_instance_name"]
            INSTANCE_CONFIG["LockFilename"] = doc['instance']["lock_filename"]
            INSTANCE_CONFIG["TagRole"] = doc['instance']["tag_role"]
            INSTANCE_CONFIG["TagSchema"] = doc['instance']["tag_schema"]
            INSTANCE_CONFIG["Environment"] = doc['instance']["tag_environment"]
            INSTANCE_CONFIG["Retention"] = doc['instance']["retention_seconds"]

            logging.debug(kayvee.formatLog("Set_mysql_config", "debug", "creating Set_mysql_config",
                                           {'context': "set_mysql_config Successfull", 'time': str(datetime.now())}))

    except Exception as e:
        logging.error(kayvee.formatLog("Set_mysql_config", "error", "creating Set_mysql_config",
                                       {'context': "set_mysql_config param not found: " + str(e),
                                        'time': str(datetime.now())}))


def _start_server():
    """
    Start Mysql server
    """
    logging.debug(kayvee.formatLog("_start_server", "debug", "Starting mysql server",
                                   {'context': "starting service ...", 'time': str(datetime.now())}))
    try:
        os.system("service mysql start")
    except (OSError, ValueError) as e:
        logging.error(kayvee.formatLog("_start_server", "error", "Starting mysql server",
                                       {'context': "error has occurred" + str(e), 'time': str(datetime.now())}))


def _stop_server(dbconnection):
    """
        Stop mysql server
        :param dbconnection:
        :return: bool
        """
    logging.info(kayvee.formatLog("Stop_server", "info", "Starting ....",
                                   {'context': "checking if server is running", 'time': str(datetime.now())}))
    try:
        if check_server_is_running(dbconnection):
            logging.info(kayvee.formatLog("Stop_server", "info", "Service mysql stopping ...",
                                           {'context': "stopping ...", 'time': str(datetime.now())}))

            os.system("service mysql stop")
            time.sleep(300)
        else:
            logging.info(kayvee.formatLog("Stop_server", "info", "Service mysql stopped",
                                           {'context': "Stopped!", 'time': str(datetime.now())}))

        return True
    except Exception as e:
        logging.error(kayvee.formatLog("Stop_server", "error", "Error!",
                                       {'context': "error:" + str(e), 'time': str(datetime.now())}))
        return False


def get_mysql_conn():
    """
    Read Database parameters from variables and create database connection object
    :return: MySQLdb
    """

    logging.debug(kayvee.formatLog("get_mysql_conn", "debug", "Starting ....",
                                   {'context': "Create mysql conn", 'time': str(datetime.now())}))
    set_mysql_config()

    try:
        return MySQLdb.connect(
            host=MYSQL_CONFIG['Host'],
            port=MYSQL_CONFIG["Port"],
            passwd=MYSQL_CONFIG["Password"],
            db=MYSQL_CONFIG["Db"],
            user=MYSQL_CONFIG["User"]
        )

    except Exception as e:
        logging.error(kayvee.formatLog("get_mysql_conn", "error", "Error!",
                                       {'context': "Error get_mysql_conn: " + str(e), 'time': str(datetime.now())}))


def get_server_lag(dbconnection):
    """
     Checking if database server has more than 10800 seconds of lag, if the number is above returns True
    :param dbconnection:
    :return:
    """

    logging.debug(kayvee.formatLog("get_server_lag", "debug", "Starting ...",
                                   {'context': "checking Server for Lag", 'time': str(datetime.now())}))

    lag_status = True
    try:
        result = mysql_query(dbconnection, 'show slave status')
        if result is None:
            print (str(datetime.now()) + " Server is not a slave")
        else:
            for row in result.fetchall():
                if row['Seconds_Behind_Master'] < 10800:
                    lag_status = False

        logging.info(kayvee.formatLog("get_server_lag", "info", "Server lag",
                                      {'context': "Lag: " + str(lag_status), 'time': str(datetime.now())}))

        return lag_status
    except Exception as e:
        logging.error(kayvee.formatLog("get_server_lag", "error", "Error",
                                       dict(context="Error: " + str(e),
                                            time=str(datetime.now()))))
        return lag_status


def check_server_is_running(dbconnection):
    """
        Check we open a connection to the server asking for the read_only flag. If process fail
        False is returned
        :param dbconnection:
        :return: bool
        """
    server_running = True
    logging.info(kayvee.formatLog("check_server_is_running", "info", "Starting ...",
                                  {'context': " ...:", 'time': str(datetime.now())}))
    result = 0
    try:
        if not get_lock_file():
            logging.debug(kayvee.formatLog("check_server_is_running", "debug", "Lock file exists ...",
                                           {'context': "Lock file NOT exists", 'time:': str(datetime.now())}))
            if dbconnection is not None:
                logging.debug(kayvee.formatLog("check_server_is_running", "debug", "Check dbconnection",
                                               {'context': "Check_server_is_running.dbconnection: "
                                                           + str(dbconnection), 'time': str(datetime.now())}))

                cursor = mysql_query(dbconnection, "select @@read_only read_only")
                result_set = cursor.fetchall()

                for row in result_set:
                    result = row["read_only"]
                    logging.debug(kayvee.formatLog("check_server_is_running", "debug", "Check Server is master",
                                                   {'context': "Check_server_is_running_is_read_only: "
                                                               + str(result), 'time': str(datetime.now())}))

                if result != 1:
                    server_running = False
                    logging.info(kayvee.formatLog("check_server_is_running", "info", "Server is Master",
                                                  {'context': "Server is a Master: " +
                                                              str(server_running), 'time': str(datetime.now())}))
                else:
                    logging.info(kayvee.formatLog("check_server_is_running", "info", "Server is Slave",
                                                  {'context': "Server is a Slave: ", 'time': str(datetime.now())}))

        return server_running
    except Exception as e:
        logging.error(kayvee.formatLog("Check_server_is_running", "error", "Error on function",
                                       {'context': "Error: " + str(e), 'time': str(datetime.now())}))

        return False


def mysql_query(conn, query):
    """
    Wrapper to connect to the database returning certain query value
    :param conn:
    :param query:
    :return: cur
    """

    logging.info(kayvee.formatLog("mysql_query", "info", "Starting...",
                                  {'context': "Wrapper for query:" + str(query), 'time': str(datetime.now())}))
    try:
        cur = conn.cursor(MySQLdb.cursors.DictCursor)
        cur.execute(query)
        return cur
    except (OSError, ValueError) as err:
        logging.error(kayvee.formatLog("mysql_query", "error", "Error on mysql_query",
                                       {'context': " Error:" + str(err), 'time': str(datetime.now())}))
        return None


def check_last_snapshots_completed(pvolumeid):
    """
    validate if any snapshot is still running
    """

    logging.debug(kayvee.formatLog("Check_last_snapshots_completed", "debug", "Starting...",
                                  {'context': "...", 'time': str(datetime.now())}))

    snapshot_completed = True
    ec2 = connect_to_region('us-east-1')
    snapshots = ec2.get_all_snapshots(filters={'volume-id': [pvolumeid]})

    for snapshot in snapshots:
        if snapshot.status != 'completed':
            snapshot_completed = False
            break
    return snapshot_completed


def check_last_snapshots_age(pvolumeid):
    """
    Check that the last snapshot (86400 sec) has more than 1 day.
    :param pvolumeid:
    :return: bool
    """
    logging.info(kayvee.formatLog("check_last_snapshots_age", "info", "Starting...",
                                  {'context': "...", 'time': str(datetime.now())}))
    snapshot_age = True
    ec2 = connect_to_region('us-east-1')
    snapshots = ec2.get_all_snapshots(filters={'volume-id': [pvolumeid]})

    for snapshot in snapshots:
        timestamp = datetime.strptime(
            snapshot.start_time,
            '%Y-%m-%dT%H:%M:%S.000Z')

        logging.info(kayvee.formatLog("check_last_snapshots_age", "info", "Check snapshot aged",
                                      {'context': "snapshot id:" + str(snapshot.id) + " timestamp:" + str(timestamp),
                                       'time': str(datetime.now())}))

        if int((datetime.utcnow() - timestamp).total_seconds()) < 86400:
            snapshot_age = False
            break

    return snapshot_age


def delete_snapshots_gt_d(pvolumeid):
    """

    :param pvolumeid:
    :return: bool
    """
    logging.debug(kayvee.formatLog("delete_snapshots_gt_d", "debug", "Starting...",
                                  {'context': "...", 'time': str(datetime.now())}))
    snapshot_age = False
    ec2 = connect_to_region('us-east-1')
    snapshots = ec2.get_all_snapshots(filters={'volume-id': [pvolumeid]})

    for snapshot in snapshots:
        timestamp = datetime.strptime(
            snapshot.start_time,
            '%Y-%m-%dT%H:%M:%S.000Z')

        last_snap_time = int((datetime.utcnow() - timestamp).total_seconds())
        logging.info(kayvee.formatLog("delete_snapshots_gt_d", "info", "last_snap_time: " + str(last_snap_time),
                                      {'context': "Get Snapshot timestamp:" + str(last_snap_time),
                                       'time': str(datetime.now())}))

        if last_snap_time > int(INSTANCE_CONFIG["Retention"]):
            logging.info(kayvee.formatLog("delete_snapshots_gt_d", "info", "Candidates to be deleted",
                                          {'context': str(snapshot.id), 'time': str(datetime.now())}))
            ec2 = boto3.client('ec2', region_name='us-east-1')
            ec2.delete_snapshot(SnapshotId=snapshot.id)
            snapshot_age = True
            time.sleep(5)
        else:
            logging.info(kayvee.formatLog("delete_snapshots_gt_d", "info", "Not candidates to be deleted",
                                          {'context': "not candidates", 'time': str(datetime.now())}))

    return snapshot_age


def get_lock_file():
    """
    check lock file exists
    :return: bool
    """
    if os.path.isfile(INSTANCE_CONFIG["LockFilename"]):
        logging.info(kayvee.formatLog("get_lock_file", "info", "Getting Lock File",
                                      {'context': " exists, process already running", 'time': str(datetime.now())}))
        return True
    else:
        return False


def set_lock_file():
    """
    Check if lock file exists,If file does exists then False value, if not create a new one and returns True
    :return:
    """
    return_value = False
    logging.debug(kayvee.formatLog("set_lock_file", "debug", "Setting Lock File",
                                  {'context': "Starting", 'time': str(datetime.now())}))
    if not get_lock_file():
        lock_file = open(INSTANCE_CONFIG["LockFilename"], 'w')
        try:
            fcntl.lockf(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
            logging.info(kayvee.formatLog("set_lock_file", "info", "Setting Lock File",
                                          {'context': "Locking process", 'time': str(datetime.now())}))
            return_value = True
        except IOError:
            logging.info(kayvee.formatLog("set_lock_file", "info", "Setting Lock File",
                                          {'context': " Cannot lock: " + INSTANCE_CONFIG["LockFilename"],
                                           'time': str(datetime.now())}))
            return return_value

    return return_value


def remove_lock_file():
    """

    :return: bool
    """
    logging.info(kayvee.formatLog("remove_lock_file", "info", "Removing lock file",
                                  {'context': "Starting", 'time': str(datetime.now())}))
    try:
        os.remove(INSTANCE_CONFIG["LockFilename"])
        return True
    except IOError:
        logging.error(kayvee.formatLog("remove_lock_file", "error", "Removing lock file",
                                       {'context': "Can not remove", 'time': str(datetime.now())}))
        return False


def create_snapshot(dbconnection):
    """

        :param dbconnection:
        :return:
        """
    return_value = False
    try:
        logging.info(kayvee.formatLog("create_snapshot", "info", "create_snapshot",
                                      {'context': "Starting", 'time': str(datetime.now())}))
        if check_server_is_running(dbconnection):
            if get_server_lag(dbconnection):
                logging.info(kayvee.formatLog("create_snapshot", "info", "create_snapshot",
                                              {'context': "Server has lag, Exit", 'time': str(datetime.now())}))
            else:
                logging.info(kayvee.formatLog("create_snapshot", "info", "create_snapshot",
                                              {'context': " Server has Not lag, continue",
                                               'time': str(datetime.now())}))

        if not check_last_snapshots_completed(INSTANCE_CONFIG["VolumeId"]):
            logging.info(kayvee.formatLog("create_snapshot", "info", "create_snapshot",
                                          {'context': "Check_last_snapshots_completed=False, Exit",
                                           'time': str(datetime.now())}))

        else:
            logging.info(kayvee.formatLog("create_snapshot", "info", "create_snapshot",
                                          {'context': "Check_last_snapshots_completed=True, Continue",
                                           'time': str(datetime.now())}))

            if check_last_snapshots_age(INSTANCE_CONFIG["VolumeId"]):
                logging.info(kayvee.formatLog("create_snapshot", "info", "create_snapshot",
                                              {'context': "Check_last_snapshots_age=True",
                                               'time': str(datetime.now())}))

                instances = ec2reource.instances.filter(InstanceIds=[INSTANCE_CONFIG["InstanceId"]])
                set_lock_file()
                _stop_server(dbconnection)

                if check_server_is_running(dbconnection):
                    return False

                if not check_server_is_running(dbconnection):
                    logging.info(kayvee.formatLog("create_snapshot", "info", "check server mysql server is stooped",
                                                  {'context': "Can not backup mysql running.",
                                                   'time': str(datetime.now())}))
                    return False

                for instance in instances:
                    volumes = instance.volumes.filter(VolumeIds=[INSTANCE_CONFIG["VolumeId"], ], )

                    logging.info(kayvee.formatLog("create_snapshot", "info", "create_snapshot",
                                                  {'context': "Volume_id:" + str(volumes),
                                                   'time': str(datetime.now())}))
                    for v in volumes:
                        # logging.info(kayvee.formatLog(str(datetime.now()), "info", " volume_id: " + str(v.id)))
                        # SlackManager.send_message("Starting Snapshot: volume_id: " + str(v.id))
                        snapshot = ec.create_snapshot(VolumeId=v.id, Description="Lambda backup for ebs " + v.id)
                        time.sleep(2)
                        ec.create_tags(
                            Resources=[
                                snapshot['SnapshotId'],
                            ],
                            Tags=[
                                {'Key': 'Name',
                                 'Value': INSTANCE_CONFIG["InstanceName"]},
                                {'Key': 'role', 'Value': INSTANCE_CONFIG["TagRole"]},
                                {'Key': 'schema', 'Value': INSTANCE_CONFIG["TagSchema"]},
                                {'Key': 'Environment', 'Value': INSTANCE_CONFIG["Environment"]},
                            ]
                        )

                        result = snapshot["SnapshotId"]
                        logging.info(kayvee.formatLog("create_snapshot", "info", "creating snapshots",
                                                      {'context': str(result), 'time': str(datetime.now())}))
                        return_value = True
            else:
                logging.info(kayvee.formatLog("create_snapshot", "info", "creating snapshots",
                                              {'context': "Check_last_snapshots_age=False",
                                               'time': str(datetime.now())}))

        return return_value

    except (OSError, ValueError) as err:
        logging.error(kayvee.formatLog("create_snapshot", "error", "creating snapshots",
                                       {'context': "Check_last_snapshots_age=False" + str(err),
                                        'time': str(datetime.now())}))


def run():
    """

    """
    conn = get_mysql_conn()

    create_snapshot(conn)

    delete_snapshots_gt_d(INSTANCE_CONFIG["VolumeId"])

    if check_last_snapshots_completed(INSTANCE_CONFIG["VolumeId"]) and get_lock_file() and conn is None:
        logging.info(kayvee.formatLog("Snapshot Completed", "info", "second step reboot mysql",
                                      {'context': "rebooting mysql", 'time': str(datetime.now())}))
        _start_server()
        remove_lock_file()

    logging.info(kayvee.formatLog("Process", "info", ">>>>>>>>>>>>>>>>>>>>> End Process <<<<<<<<<<<<<<<<<<<<<<<",
                                  {'context': "Exit Process", 'time': str(datetime.now())}))


def main():
    while True:
        logging.info(kayvee.formatLog("Main", "info", ">>>>>>>>>>>>>>>>>>>>> Starting <<<<<<<<<<<<<<<<<<<<<<<",
                                      {'context': "...", 'time': str(datetime.now())}))
        run()
        time.sleep(3600)


if __name__ == "__main__":
    logging.basicConfig(filename='/tmp/offline_backup.log', level=logging.INFO)
    main()
