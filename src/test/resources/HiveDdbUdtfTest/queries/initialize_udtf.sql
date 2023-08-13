create database if not exists klimber;
use klimber;

create temporary function ddb_query as 'com.klimber.hiveddbudtf.MockedHiveDdbQueryUdtf';
