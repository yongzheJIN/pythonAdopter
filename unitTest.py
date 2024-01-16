# from dataFusion.mysqlConnector.mysqlConnector import mysqlConnector
#
# with mysqlConnector(ip="127.0.0.1", port=3306, user="root", password="123456", database="xex_plus_qd") as connector:
#     for i in range(1000, 2000):
#         querySentence = f"""INSERT INTO `xex_plus_qd`.`xex_home_order` (`id`, `order_id`, `order_no`, `customer_id`, `service_object_id`, `service_provider_id`, `service_person_id`, `state`, `order_type`, `number`, `price`, `spotprice`, `product_number`, `order_time`, `service_time`, `estimate_service_time`, `business_time`, `start_time`, `end_time`, `actual_service_time`, `evaluate_id`, `create_time`, `update_time`, `service_word`, `sequence`, `cancel_reason`, `pricing_reason`, `cancel_reson_detail`, `order_remark`, `order_nature`, `start_address`, `end_address`, `order_reservation_type`, `nature_type_id`, `appoint_time`, `is_pay`, `pay_time`, `order_over_time`, `is_visit`, `visit_state`, `visit_person_id`, `visit_id`, `gov_mode_id`, `gov_older_type_id`, `is_confirm`, `is_alarm_order`, `plan_id`, `service_object_name`, `service_object_phone`, `service_object_id_number`, `service_object_region_code`, `service_object_address`, `charge_unit`, `sign_time`, `category`, `is_no_formal`, `real_service_iteams`, `warn_deal`, `warn_deal_remark`, `send_price`, `gov_subsidy_type_id`, `gov_subsidy_price`, `subsidy_price`, `nurse_project_id`, `date_source`, `xbx_id`, `is_subsidy`, `person_id`, `emp_id`, `is_over_area`, `is_food_resign`, `ip`, `room_id`, `public_person_id`, `over_distance`, `warn_info`, `terminal_type`, `volunteer_type`, `member_id`, `service_start_time`, `service_end_time`, `service_score`, `lat`, `lng`, `nurse_project_name`, `institution_name`, `employee_name`, `order_pic`, `start_video`, `end_video`, `order_video`, `ins_id`, `creater_id`, `ins_level_way`, `subsidy_id`, `person_type`, `card_type_id`, `card_type_name`, `card_type_source_code`, `buy_server_id`, `invoice_status`, `operation_center`, `older_type_id`, `older_type_name`, `project_remake`, `older_price`, `older_duration`, `older_minute`, `device_mac`, `has_video`, `sign_up`, `year_quarter`, `kitchen_id`, `station_id`, `station_name`, `region_subsidy`, `street_subsidy`, `community_subsidy`, `is_agree`, `record_video_status`) VALUES ({i}, {i}, '20231228170373531053876c816db', NULL, '309971', '63de30d42d2a4844b058039867b60a68', NULL, '8', '7', NULL, '9', '17', '1', '2023-12-28 11:48:30', '2023年12月28日11:48-11:48', NULL, '0000-00-00 00:00:00', '2023-12-28 11:48:30', '2023-12-28 11:48:30', NULL, NULL, '2023-12-28 11:48:11', '2023-12-28 11:48:11', NULL, '2', NULL, NULL, NULL, NULL, NULL, NULL, NULL, '0', NULL, NULL, '1', NULL, NULL, '0', '0', NULL, NULL, '3', NULL, NULL, '0', NULL, '孟根吉', '13704772653', '152727195411290029', '150626100006000', NULL, '1', NULL, NULL, '0', '4.00', NULL, NULL, NULL, NULL, '9.00', '9', '9999', '0', NULL, '1', NULL, NULL, NULL, '0', NULL, NULL, NULL, '0', NULL, '81', '0', NULL, '2023-12-28 11:48:30', '2023-12-28 11:48:30', NULL, NULL, NULL, '助餐', '嘎鲁图镇南丁社区乌嘟太蒙餐馆助餐点', '集中助餐', NULL, NULL, NULL, NULL, '9489', NULL, '9488/9489/', NULL, '3', NULL, NULL, '', NULL, '0', '0', NULL, NULL, NULL, NULL, NULL, '0', NULL, '0', '0', NULL, NULL, NULL, NULL, '4', '5', '0', NULL, '0');
#     """
#         cursor = connector.cursor()
#         querySentence2 = f"""INSERT INTO `xex_plus_qd`.`institution` (`id`, `gt_orang_id`, `name_en`, `institution_name`, `institution_phone`, `institution_number`, `postcode`, `service_range`, `principal`, `principal_phone`, `legal_representative`, `legal_representative_phone`, `institution_type`, `certificate_number`, `institution_nature`, `operate_way`, `legal_representative_number`, `house_nature`, `is_fire_check`, `hygienic_license`, `bank_account`, `third_comm_id`, `standard_bill`, `address`, `service_object_home`, `doctor_house`, `is_independent_totile`, `single_bed_area`, `activity_area`, `recovery_area`, `logistics_area`, `mess_area`, `work_area`, `outdoor_area`, `build_area`, `cover_area`, `elevator`, `region_code`, `parent_id`, `level_way`, `delflag`, `menber_grade_ids`, `create_time`, `update_time`, `administrative_areas`, `img_url`, `flow_set`, `month_report_date`, `institution_synopsis`, `bed_number`, `service_type`, `service_object`, `house_area`, `cost_interval`, `bed_cost`, `nurse_cost`, `restaurant_cost`, `is_deposit`, `is_medical`, `own_medical`, `periphery_medical`, `green_channel`, `region_Address`, `statue`, `legal_representative_IDcard`, `logo_url`, `business_licence_url`, `date_source`, `xbx_id`, `services_id`, `id_number`, `merchant_type`, `pos_logt`, `pos_lat`, `is_examine`, `services_code`, `institutional_details_id`, `statues`, `geo_code`, `detail_address`, `out_plan`, `ins_older_type_str`, `index_data`, `full_data_manual`, `start_business_time`, `invested_funds`, `establish_licence`, `establish_time`, `arrearage`, `company_name`, `is_edit`, `is_operate`, `volunteer_audit`, `card_interval_time`, `institution_code`, `service_region_code`, `language_type`, `gov_order_over_time`, `serivice_type_set`, `return_visit_cycle`, `is_over_distance`, `call_device`, `over_distance`, `breakfast_time`, `lunch_time`, `dinner_time`, `bedtime_time`, `last_review_time`, `is_balance_clear`, `activity_type_set`, `delivery_station`, `source`, `source_state`, `serial_number`, `holiday_charge`, `ask_leave_charge`, `small_menumage`, `cms_model`, `cms_logo`, `cms_qrcode`, `cms_domain`, `cms_icp`, `is_place_independent`, `is_center_multiple_site`, `is_set_old_school`, `is_set_old_course`, `old_course_detail`, `care_detail`, `qr_code`, `timecoin_ratio`, `total_bed`, `institution_plus_id`, `prne_number`, `is_helpmeal`, `floor_number`, `entertainment_type`, `entertainment_area`, `culturalactivity_type`, `culturalactivity_area`, `psycou_type`, `psycou_area`, `healcare_type`, `healcare_area`, `lifecare_type`, `lifecare_area`, `helpmeal_type`, `helpmeal_area`, `helpshower_type`, `helpshower_area`, `haircut_type`, `haircut_area`, `washclothe_type`, `washclothe_area`, `ins_type`, `work_time`, `warning_time`, `food_mode`, `send_price`, `food_price`, `food_time`, `food_limit`, `call_before_num`, `help_meal_min_money_setting`, `help_meal_min_money`, `check_bed_num`, `certificate_accessory`, `establish_accessory`, `hygienic_accessory`, `fire_check_accessory`, `special_equipment_accessory`, `special_equipment`, `org_image`, `reward`, `checkin_period`, `is_open`, `is_attend`, `is_service_providers`, `activity_room`, `call_gate_way`, `self_assess_score`, `self_assess_lev`, `self_assess_id`, `assess_account`, `isOpenAssess`, `is_open_ordetTerminal`, `show_state`, `reserved_phone`, `complete_order_edit`, `service_status`, `meal_status`, `is_change`, `is_eat_room`, `is_cook_room`, `is_medication`, `is_care`, `is_record`, `subsidy_interface`, `detail_type`, `kitchen_id`, `business_hours`, `business_person`, `help_meal_state`, `center_way`, `need_card`, `wx_content`, `wx_img_url`, `valid`, `status_change_time`, `is_food_license`, `org_type`, `medical_insurance_no_temp`, `third_client_id`, `isExchange`, `isNeedExchange`, `operate_name`, `operate_determine_way`, `commencement_date`, `completion_date`, `ysf_status`, `construction_status`, `before_work_pic`, `work_pic`, `end_work_pic`, `is_in_use`, `allow_appointment_time`, `region_type`, `filing_number`, `feeding_way`, `is_install_bank_device`, `operate_agreement_pic`, `approval_status`, `notice_file`, `ins_file`, `affairs_year`, `is_canteen`, `filing_application`, `business_hours_json`, `elderly_assistance_set`, `specialty_dishes_json`, `subsidy_price`, `brief_introduction`, `avg_score`, `community_level`) VALUES ({i}, NULL, NULL, '中科西北星养老机构(集团)22', '111111111', NULL, NULL, NULL, '刘雷', '18036001118', NULL, NULL, '0', NULL, '0', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '', NULL, NULL, NULL, '7013', '7013/', '0', NULL, '2020-10-22 19:34:37', '2022-04-18 14:20:04', NULL, '/fpng/20201022/www-1603366493482.png', NULL, '0', NULL, '', '自理服务;半自理服务;非自理服务;日托服务;居家服务;', '', '', '', '', '', '', NULL, NULL, '', '', '', '', '2', NULL, '/fpng/20201110/www-1605000498617.png', '/fpng/20201022/www-1603366428928.png', NULL, NULL, '00037f774a154494a0a8c10a08b23ee5', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '0', NULL, '1111', '0', NULL, NULL, NULL, NULL, '2', '1', '1', NULL, '1', '10', NULL, NULL, '0', '0', NULL, '0', '0', '0', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '0', NULL, NULL, NULL, '0', '0', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '0.00', NULL, '1', NULL, NULL, NULL, '0', NULL, '0', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '0.00', '0', '0', '0', '0', NULL, NULL, NULL, NULL, NULL, NULL, NULL, '0', '1', NULL, '0', '1', '1', '0', '0', '0', '0', '0', '0', NULL, '0', NULL, NULL, NULL, '0', NULL, '0', NULL, NULL, '0', NULL, NULL, NULL, NULL, NULL, '0', '0', NULL, NULL, NULL, NULL, '0', NULL, NULL, NULL, NULL, NULL, NULL, '0', NULL, NULL, NULL, NULL, NULL, NULL, NULL, '0', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);"""
#         cursor.execute(querySentence)
#         cursor.execute(querySentence2)
#     connector.commit()
import datetime

from dataFusion.mysqlConnector.mysqlConnector import mysqlConnector


def findColumnsOfsql(targetTable):
    """
    找到某一个表格的所哟字段，包括哪些是primary key
    """
    with mysqlConnector(ip="192.168.1.152", port=30633, user="root",
                        password="Zkxbx@2011", database="xex_plus") as connector:
        cursors = connector.cursor()
        try:
            sql = f"""DESCRIBE xex_plus.{targetTable}"""
            cursors.execute(sql)
            original_columns = []
            key_columns = []
            for i in cursors.fetchall():
                original_columns.append(str(i[0]))
                if str(i[3]) == "PRI":
                    key_columns.append(i[0])
            if len(key_columns) == 0:
                print(targetTable)
                return targetTable
        except:
            None
            # print("没有检查", targetTable)
        # return original_columns, key_columns


# 查询所有的table
def getALLtables():
    """
    获取所有的表格
    """
    with mysqlConnector(ip="192.168.1.152", port=30633, user="root",
                        password="Zkxbx@2011", database="xex_plus") as connector:
        cursors = connector.cursor()
        sql = f"""SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'xex_plus' AND table_type = 'BASE TABLE';"""
        cursors.execute(sql)
        original_columns = []
        for i in cursors.fetchall():
            original_columns.append(str(i[0]))
        return original_columns


# res = getALLtables()
# for i in range(len(res)):
#     findColumnsOfsql(res[i])
print(f"现在是{datetime.datetime.now()}")