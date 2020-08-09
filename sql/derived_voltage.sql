select count(*),trunc(vt.time_stamp) date_key,max(mt.ID)ID ,vt.node_scada_name,min(mt.node_name)Node_name,min(vt.voltage_value) min,max(vt.voltage_value) max,avg(vt.voltage_value) avg
from mapping_table mt,raw_voltage vt 
where mt.NODE_SCADA_NAME = vt.NODE_SCADA_NAME and vt.time_stamp between to_date('2019-07-22 00:00:00') and to_date('2019-07-23 23:59:00')
group by vt.node_scada_name,trunc(vt.time_stamp)
order by date_key,ID
