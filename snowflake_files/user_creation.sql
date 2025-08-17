use role accountadmin;

create role if not exists transform;
grant role transform to role accountadmin;

create warehouse if not exists compute_wh;
grant operate on warehouse compute_wh to role transform;

create user dbt
    password = 'shree'
    DEFAULT_NAMESPACE = movielens.raw
    default_warehouse = compute_wh
    default_role = transform
    comment = 'dbt user for the transformation'
    must_change_password = false;
alter user set type = legacy_service;
grant role transform to user dbt;

create database if not exists movielens;
create schema if not exists movielens.raw;

grant all on warehouse compute_wh to role transform;
grant all on database movielens to role transform;
grant all on all schemas in database movielens to role transform;
grant all on future schemas in database movielens to role transform;
grant all on all tables in schema movielens.raw to role transform;
grant all on future tables in schema movielens.raw to role transform;










