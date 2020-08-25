create table derived_VDI(
id NUMBER GENERATED BY DEFAULT ON NULL AS IDENTITY,
mapping_id number,
week_start_date date,
node_name varchar(20),
node_voltage number,
maximum number,
minimum number,
less_than_band number(8,3),
between_band number(8,3),
greater_than_band number(8,3),
less_than_band_inHrs number(8,3),
greater_than_band_inHrs number(8,3),
out_of_band_inHrs number(8,3),
VDI number(8,3),
constraints unique_derived_VDI unique(week_start_date,node_name),
constraints fk_derived_VDI foreign key(mapping_id) references voltage_mapping_table(id),
constraints pk_derived_VDI primary key(id)
)