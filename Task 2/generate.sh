#!/bin/bash
datamodel-codegen --input openapi/openapi.yaml --input-file-type openapi --output src/models/generated.py --output-model-type pydantic_v2.BaseModel