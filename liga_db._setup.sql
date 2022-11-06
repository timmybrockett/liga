create database liga;
\connect liga

create schema stats;

create table stats.clubs (
            id serial       primary key,
            club            varchar unique,
            mv_euros        numeric,
            num_foreigners  int
            );

create table stats.players (
            id serial           primary key,
            player_name         varchar unique,
            status              varchar,
            primary_position    varchar,
            secondary_position  varchar,
            age                 int,
            country             varchar,
            club                varchar,
            mv_euros            int
            );

