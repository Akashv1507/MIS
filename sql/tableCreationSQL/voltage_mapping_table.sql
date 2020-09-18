CREATE TABLE VOLTAGE_MAPPING_TABLE (
	ID NUMBER GENERATED BY DEFAULT ON NULL AS IDENTITY,
	SCADA_ID VARCHAR2(100),
	NODE_VOLTAGE NUMBER,
	NODE_NAME VARCHAR2(50),
	NODE_FULL_NAME VARCHAR2(50),
	NODE_SCADA_NAME VARCHAR2(50) UNIQUE,
	IS_INCLUDED_IN_DAILY_VOLTAGE CHAR(1),
	IS_INCLUDED_IN_WEEKLY_VDI CHAR(1),
	constraints pk_mapping_volt primary key(ID)
)