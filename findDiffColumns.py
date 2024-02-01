from dataFusion.mysqlConnector.mysqlConnector import mysqlConnector
# 需要找到不同的表
tableGroup = ['activity_manager', 'activity_person', 'activity_title', 'agency_reports', 'assets_info', 'bed',
                'build_situation_info', 'building', 'card', 'card_type', 'detection_record', 'device',
                'device_badge_msg', 'device_basic_msg', 'device_call_msg', 'device_door_msg', 'device_gas_msg',
                'device_gps_msg', 'device_info', 'device_infrared_msg', 'device_menu', 'device_smoke_msg',
                'device_soaking_msg', 'elder_family', 'employee', 'epidemic_detection', 'floor',
                'home_person_assessment', 'inelder_health', 'institution', 'institution_plus', 'institution_plus_st',
                'monitor',  'nurse_level', 'nurse_project', 'nurse_record', 'nurse_type', 'person',
                'person_account', 'person_base', 'person_bed_bind', 'person_sign_hardware', 'person_type_change_log',
                'room', 'room_type', 'security_incidents', 'security_inspect_assest_relation',
                'security_inspect_employee_relation', 'security_inspect_plan', 'security_inspect_register',
                'security_inspect_time_relation', 'subsidy_standards', 'vaccine_record', 'volunteer',
                'volunteer_institution', 'watch_bind', 'xex_home_evaluate', 'xex_home_service_object_gov',
                'xex_home_visit', 'xex_order_video']

def findDiffernt(originHost,originPort,originUser,originPassword,originDatabase,tableName,targetHost,targetPort,targetUser,targetPassword,targetDatabase):
    originColumns = []
    targetColumns = []
    with mysqlConnector(ip=originHost, port=originPort, user=originUser,
                        password=originPassword,
                        database=originDatabase) as connector:
        cursors = connector.cursor()
        sql = f"""DESCRIBE {originDatabase}.{tableName}"""
        cursors.execute(sql)
        for i in cursors.fetchall():
            originColumns.append(str(i[0]))

    with mysqlConnector(ip=targetHost, port=targetPort, user=targetUser,
                        password=targetPassword,
                        database=targetDatabase) as connector:
        cursors = connector.cursor()
        sql = f"""DESCRIBE {targetDatabase}.{tableName}"""
        cursors.execute(sql)
        for i in cursors.fetchall():
            targetColumns.append(str(i[0]))
    result = set(targetColumns) - set(originColumns)
    if result!={"client_id"} and result!={"clientId"} and result!={"ClientId"}:
        print(tableName,result)


# 目标数据库连接方式，和源库连接方式
for i in tableGroup:
    findDiffernt(originHost="220.179.5.197",originPort=8689,originUser="root",originPassword="Zkxbx@2011",originDatabase="xex_plus",
                 tableName=i,targetHost="220.179.5.197",targetPort=8689,targetUser="root",targetPassword="Zkxbx@2011",targetDatabase="civil_admin_aq")