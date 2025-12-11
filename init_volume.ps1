#!/bin/bash
echo "Initialisation du volume Presto..."
docker run --rm -v procurement_pipeline_presto_data:/data alpine:latest sh -c "mkdir -p /data && chown -R 1000:1000 /data"
