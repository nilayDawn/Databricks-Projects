-- Databricks notebook source
create catalog if not exists fmgc;
use catalog fmgc;

-- COMMAND ----------

create schema if not exists fmgc.bronze;
create schema if not exists fmgc.silver;
create schema if not exists fmgc.gold;

-- COMMAND ----------

