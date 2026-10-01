# Databricks notebook source
# False branch: refuse to publish and say why
raise Exception("Quality gate failed: dropped_records exceeded threshold. Gold NOT promoted.")
