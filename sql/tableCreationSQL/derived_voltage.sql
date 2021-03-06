create table derived_voltage (
    id NUMBER GENERATED BY DEFAULT ON NULL AS IDENTITY,
    date_key date,
    mapping_id number,
    node_scada_name varchar2(50),
    node_name varchar2(50),
    minimum number,
    maximum number,
    average number,
    constraints unique_derived_volt unique(date_key, node_scada_name),
    constraints fk_derived_volt foreign key(mapping_id) references voltage_mapping_table(id),
    constraints pk_derived_volt primary key(id)
)