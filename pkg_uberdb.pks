create or replace 
PACKAGE PKG_UBERDB AS 

  PROCEDURE PRC_INSERT_OR_UPDATE (
    P_KEY VARCHAR2,
    P_VALUE CLOB
  );

END PKG_UBERDB;
