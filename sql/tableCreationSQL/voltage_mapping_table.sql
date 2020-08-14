CREATE TABLE MAPPING_TABLE
   (
	ID NUMBER GENERATED BY DEFAULT ON NULL AS IDENTITY, 
	SCADA_ID VARCHAR2(100), 
	NODE_VOLTAGE NUMBER, 
	NODE_NAME VARCHAR2(50), 
	NODE_FULL_NAME VARCHAR2(50), 
	NODE_SCADA_NAME VARCHAR2(50), 
	IS_INCLUDED_IN_WEEKLY CHAR(1)
   )
