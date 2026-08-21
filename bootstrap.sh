#!/usr/bin/env bash
# Recreate the de-mastery learning repo skeleton in the current directory.
set -euo pipefail
mkdir -p curriculum/syllabus class-sessions notebooks references exercises labs projects \
         architecture-studies tools/{sql,spark,airflow,dbt,kafka,warehouse-lakehouse,cloud,iac} \
         assessments/results review tracker sessions scripts
find curriculum class-sessions notebooks references exercises labs projects architecture-studies \
     tools assessments review tracker sessions scripts -type d -empty -exec touch {}/.gitkeep \;
echo "Skeleton created. Add the authoritative docs (CLAUDE.md, curriculum/, tracker/, assessments/rubric.md)."
