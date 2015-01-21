create or replace 
PACKAGE BODY PKG_UBERDB AS

  PROCEDURE PRC_INSERT_OR_UPDATE (
    P_KEY VARCHAR2,
    P_VALUE CLOB
  ) AS
    CURSOR C_STORE IS
      SELECT COUNT(*) cnt 
        FROM STORE 
       WHERE key = P_KEY;     
    L_CNT NUMBER;
    
  BEGIN
    OPEN C_STORE;
    FETCH C_STORE INTO L_CNT;
    IF (L_CNT > 0) THEN
      UPDATE STORE SET value = P_VALUE where key = P_KEY;
    ELSE
      INSERT INTO STORE (key, value) values (P_KEY, P_VALUE);
    END IF;
    commit;
  END PRC_INSERT_OR_UPDATE;
END PKG_UBERDB;
