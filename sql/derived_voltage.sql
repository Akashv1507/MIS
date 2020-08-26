-- select (date_key, mapping_id, Node_SCADA_name, node_name, minimum, maximum, average) from cross product of mapping table and raw voltage table
select trunc(vt.time_stamp) date_key,
    max(mt.ID) mapping - id,
    vt.node_scada_name,
    min(mt.node_name) Node_name,
    min(vt.voltage_value) minimum,
    max(vt.voltage_value) maximum,
    avg(vt.voltage_value) average
from mapping_table mt,
    raw_voltage vt
where mt.NODE_SCADA_NAME = vt.NODE_SCADA_NAME
    and vt.time_stamp between to_date('2019-07-22 00:00:00') and to_date('2019-07-23 23:59:00')
group by vt.node_scada_name,
    trunc(vt.time_stamp)
order by date_key,
    ID