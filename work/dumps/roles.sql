--
-- PostgreSQL database cluster dump
--

\restrict z8GGqeMfnjuOHlIGlDMzrGlYHbFs28MI8gRDewbiBrEOQe9i74OHWllWKr9yN2R

SET default_transaction_read_only = off;

SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;

--
-- Roles
--

CREATE ROLE migrator;
ALTER ROLE migrator WITH SUPERUSER INHERIT CREATEROLE CREATEDB LOGIN REPLICATION BYPASSRLS PASSWORD 'SCRAM-SHA-256$4096:PW4q4mA/FLeONiKB6evevQ==$Av5BT8HEf43KSd/mzPDkarT7Fpj9bIFtz5Qc3OpKPMk=:rwhoFPPXplE9Ozu45RLp41gB9CYIHwTtk4W7I7/A4kE=';

--
-- User Configurations
--








\unrestrict z8GGqeMfnjuOHlIGlDMzrGlYHbFs28MI8gRDewbiBrEOQe9i74OHWllWKr9yN2R

--
-- PostgreSQL database cluster dump complete
--

