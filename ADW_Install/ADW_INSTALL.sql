--
-- This script will set up the Application Data Warehouse
--	Requires and account ADWADMIN which has DBA Privs
--
-- Copy into SQL Developer worksheet
-- Change CD to your install directory
-- Run script
--
cd C:\ADW\ADW_Install
spool ADW_INSTALL.log
--
--
PROMPT Application Data Warehouse Installation
PROMPT
PROMPT   -- ADW Tables and Sequences
PROMPT
--
--
@ADW_SEQUENCES.sql

@ADW_TABLES.sql

@ADW_PACKAGES.sql

@ADW_INITIALIZE.sql

PROMPT --- Installation Completed ---

spool off
