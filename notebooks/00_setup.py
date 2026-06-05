# Databricks notebook source
# 00_setup.py
# Run this once to create the Unity Catalog structure

# %sql
# CREATE CATALOG IF NOT EXISTS kali_demo;
# USE CATALOG kali_demo;
# CREATE SCHEMA IF NOT EXISTS bronze;
# CREATE SCHEMA IF NOT EXISTS silver;
# CREATE SCHEMA IF NOT EXISTS gold;