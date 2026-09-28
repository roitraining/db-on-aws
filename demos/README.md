# Demos

Instructor demonstration notebooks. These are shown live in class; they are here so you can re-read them afterwards.

- `spark_ui_showcase.py` — partition sizes, shuffle, skew, spill, and OOM, each producing one artifact in the Spark UI. Requires a **classic cluster** (the Spark UI does not exist on serverless, and Free Edition cannot create classic compute), so in class this runs on the instructor's shared cluster. The final cell fails on purpose.
