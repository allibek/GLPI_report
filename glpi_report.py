import pandas as pd
from openpyxl import load_workbook
from mysql.connector import (connection)

if __name__ == "__main__" :
    glpi_connection = connection.MySQLConnection(user='', password='', host='10.32.100.82', database='glpi')
    entities = pd.read_sql('select id, name, name as names, comment from glpi_entities order by (comment * 1)', con=glpi_connection)
    pc = pd.read_sql('select id, name, otherserial, entities_id, computertypes_id, states_id from glpi_computers where is_deleted = 0 and otherserial is not null and otherserial != ""', con=glpi_connection)
    printers = pd.read_sql('select id, name, otherserial, entities_id, printertypes_id  from glpi_printers where is_deleted = 0 and otherserial is not null and otherserial != ""', con=glpi_connection)
    printers_speed = pd.read_sql('select id, items_id, (printspeedfield * 1) as printspeedfield, technologicalprintersfield, plugin_fields_papersizefielddropdowns_id from  glpi_plugin_fields_printerprinters', con=glpi_connection)
    scaners = pd.read_sql('select id, name, otherserial, entities_id from glpi_peripherals where peripheraltypes_id = 3 and is_deleted = 0 and otherserial is not null and otherserial != ""', con=glpi_connection)
    scaners_speed = pd.read_sql('select id, items_id, (scanspeedfield * 1) as scanspeedfield, plugin_fields_scanertypefielddropdowns_id, devicetechnologicalfield from glpi_plugin_fields_peripheraldevices', con=glpi_connection)
    ups = pd.read_sql('select id, name, otherserial, entities_id from glpi_peripherals where peripheraltypes_id = 5 and is_deleted = 0 and otherserial is not null and otherserial != ""', con=glpi_connection)
    processors = pd.read_sql('select id, items_id, deviceprocessors_id, entities_id, nbcores, frequency from glpi_items_deviceprocessors where is_deleted = 0', con=glpi_connection)
    technologicals = pd.read_sql('select items_id from glpi_plugin_fields_computertechnologicals where technologicalfield = 1;', con=glpi_connection)
    glpi_connection.close()


    result_data = pd.DataFrame()
    for index, entity in entities.iterrows():
        computers = pc[(pc.entities_id == entity['id']) & (pc.computertypes_id == 1)]
        notebooks = pc[(pc.entities_id == entity['id']) & (pc.computertypes_id == 9)]
        technological_computers = computers[computers.id.isin(technologicals.items_id)&(computers.states_id == 1)]
        zero_core_computers = computers[computers.id.isin(processors[processors.nbcores.isnull()].items_id)]
        three_core_computers = computers[computers.id.isin(processors[processors.nbcores == 3].items_id)]
        one_core_computers = computers[computers.id.isin(processors[processors.nbcores == 1].items_id)]
        two_core_computers_le_3000 = computers[computers.id.isin(processors[(processors.nbcores == 2) & (processors.frequency <= 3000)].items_id)]
        two_core_computers_gt_3000 = computers[computers.id.isin(processors[(processors.nbcores == 2) & (processors.frequency > 3000)].items_id)]
        four_ge_core_computers = computers[computers.id.isin(processors[processors.nbcores >= 4].items_id)]
        one_core_notebooks = notebooks[notebooks.id.isin(processors[processors.nbcores == 1].items_id)]
        two_core_notebooks_le_2300 = notebooks[notebooks.id.isin(processors[(processors.nbcores == 2) & (processors.frequency <= 2300)].items_id)]
        two_core_notebooks_gt_2300 = notebooks[notebooks.id.isin(processors[(processors.nbcores == 2) & (processors.frequency > 2300)].items_id)]
        four_ge_core_notebooks = notebooks[notebooks.id.isin(processors[processors.nbcores >= 4].items_id)]
        problem_computers = pc[(pc.entities_id == entity['id']) & (~computers.id.isin(processors.items_id))]
        entity_printers = printers[(printers.entities_id == entity['id']) & (printers.printertypes_id == 1)]
        entity_printers_speed_lt_25 = entity_printers[entity_printers.id.isin(printers_speed[printers_speed.printspeedfield < 25].items_id)]
        entity_printers_speed_25_40 = entity_printers[entity_printers.id.isin(printers_speed[(printers_speed.printspeedfield >= 25) & (printers_speed.printspeedfield < 40)].items_id)]
        entity_printers_speed_ge_40 = entity_printers[entity_printers.id.isin(printers_speed[(printers_speed.printspeedfield >= 40)].items_id)]
        entity_matrix_printers = printers[(printers.entities_id == entity['id']) & (printers.printertypes_id == 5)]
        entity_jet_printers = printers[(printers.entities_id == entity['id']) & (printers.printertypes_id == 4)]
        multifunction_devices = printers[(printers.entities_id == entity['id']) & (printers.printertypes_id == 2)]
        mobile_printers = printers[(printers.entities_id == entity['id']) & (printers.printertypes_id == 8)]
        problem_printers = entity_printers[entity_printers.id.isin(printers_speed[(printers_speed.plugin_fields_papersizefielddropdowns_id == 0) | (printers_speed.printspeedfield == 0)].items_id)]
        entity_scaners = scaners[scaners.entities_id == entity['id']]
        entity_flatbad_scaners = entity_scaners[entity_scaners.id.isin(scaners_speed[scaners_speed.plugin_fields_scanertypefielddropdowns_id == 1].items_id)]
        entity_flow_scaners = entity_scaners[entity_scaners.id.isin(scaners_speed[scaners_speed.plugin_fields_scanertypefielddropdowns_id == 2].items_id)]
        entity_flatbad_scaners_speed_le_5 = entity_flatbad_scaners[entity_flatbad_scaners.id.isin(scaners_speed[scaners_speed.scanspeedfield <= 5].items_id)]
        entity_flatbad_scaners_speed_gt_5 = entity_flatbad_scaners[entity_flatbad_scaners.id.isin(scaners_speed[scaners_speed.scanspeedfield > 5].items_id)]
        entity_flow_scaners_speed_ge_25 = entity_flow_scaners[entity_flow_scaners.id.isin(scaners_speed[scaners_speed.scanspeedfield >= 25].items_id)]
        entity_flow_scaners_speed_lt_25 = entity_flow_scaners[entity_flow_scaners.id.isin(scaners_speed[scaners_speed.scanspeedfield < 25].items_id)]
        entity_ups = ups[ups.entities_id == entity['id']]

        tmp_df = pd.DataFrame(index=[entity.name])
        tmp_df["Район"] = entity.names
        tmp_df["Код района"] = entity.comment
        tmp_df["Общее число компьютеров"] = len(computers)
        tmp_df["Число компьютеров с одноядерным процессором"] = len(one_core_computers)
        tmp_df["Число компьютеров с двухядерным процессором и частотой <= 3000"] = len(two_core_computers_le_3000)
        tmp_df["Число компьютеров с двухядерным процессором и частотой > 3000"] = len(two_core_computers_gt_3000)
        tmp_df["Число компьютеров с числом ядер процессора >= 4"] = len(four_ge_core_computers)
        tmp_df["Список компьютеров без процессора"] = problem_computers[['id', 'name', 'otherserial']].to_string()
        tmp_df["Список компьютеров с процессором где число ядер = 0"] = zero_core_computers[['id', 'name', 'otherserial']].to_string()
        tmp_df["Список компьютеров с процессором где число ядер = 3"] = three_core_computers[['id', 'name', 'otherserial']].to_string()
        tmp_df["Список одноядерных компьютеров"] = one_core_computers[['id', 'name', 'otherserial']].to_string()
        tmp_df["Список двухядерных компьютеров с частотой <=3000"] = two_core_computers_le_3000[['id', 'name', 'otherserial']].to_string()
        tmp_df["Список двухядерных компьютеров с частотой >3000"] = two_core_computers_gt_3000[['id', 'name', 'otherserial']].to_string()
        tmp_df["Список компьютеров с числом ядер процессора >= 4"] = four_ge_core_computers[['id', 'name', 'otherserial']].to_string()
        tmp_df["Общее число ноутбуков"] = len(notebooks)
        tmp_df["Число ноутбуков с одноядерным процессором"] = len(one_core_notebooks)
        tmp_df["Число ноутбуков с двухядерным процессором и частотой <= 2300"] = len(two_core_notebooks_le_2300)
        tmp_df["Число ноутбуков с двухядерным процессором и частотой > 2300"] = len(two_core_notebooks_gt_2300)
        tmp_df["Число ноутбуков с числом ядер процессора 4 и выше"] = len(four_ge_core_notebooks)
        tmp_df["Общее число принтеров"] = len(entity_printers)
        tmp_df["Число принтеров со скоростью печати до 25"] = len(entity_printers_speed_lt_25)
        tmp_df["Число принтеров со скоростью печати >= 25 и < 40"] = len(entity_printers_speed_25_40)
        tmp_df["Число принтеров со скоростью печати >= 40"] = len(entity_printers_speed_ge_40)
        tmp_df["Число матричных принтеров"] = len(entity_matrix_printers)
        tmp_df["Число струйных принтеров"] = len(entity_jet_printers)
        tmp_df["Число МФУ"] = len(multifunction_devices)
        tmp_df["Число принтеров для мобильных устройств"] = len(mobile_printers)
        tmp_df["Список принтеров с незаполнеными полями формата или скорости"] = problem_printers[['id', 'name', 'otherserial']].to_string()
        tmp_df["Общее число сканеров"] = len(entity_scaners)
        tmp_df["Общее число планшетных сканеров"] = len(entity_flatbad_scaners)
        tmp_df["Число планшетных сканеров где скорость сканирования <= 5"] = len(entity_flatbad_scaners_speed_le_5)
        tmp_df["Число планшетных сканеров где скорость сканирования > 5"] = len(entity_flatbad_scaners_speed_gt_5)
        tmp_df["Общее число поточных сканеров"] = len(entity_flow_scaners)
        tmp_df["Число поточных сканеров где скорость сканирования >= 25"] = len(entity_flow_scaners_speed_ge_25)
        tmp_df["Число поточных сканеров где скорость сканирования < 25"] = len(entity_flow_scaners_speed_lt_25)
        tmp_df["Общее число ИБП"] = len(entity_ups)
        tmp_df["Технологических компьютеров"] = len(technological_computers)
        result_data = pd.concat([result_data, tmp_df])

    writer = pd.ExcelWriter('report.xlsx')
    writer.book = load_workbook('report.xlsx')
    result_data.to_excel(writer, sheet_name='report')
    writer.save()
