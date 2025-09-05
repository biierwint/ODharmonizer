#!/usr/bin/env python3

# Copyright (C) 2025 A*STAR

# ODmapper (Omics Data Mapping and Harmonizer) is an effort by the
# Data Management Platform in the Bioinformatics Institute (BII),
# Agency of Science, Technology and Research (A*STAR), Singapore.

# This file is part of ODannotator

# ODannotator is an open-source tool.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0 you can redistribute it and/or modify
# it under the terms of the https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# You should have received a copy of the Apache License, Version 2.0
# along with this program.  If not, see <https://www.apache.org/licenses/LICENSE-2.0>


'''
This script is part of the ODharmonizer-v1.0 > ODconverter (streamlit).
It contains the function for s3 I/O-related handling.
'''

import os
import s3fs
from dotenv import load_dotenv

load_dotenv(dotenv_path=".env_apps")

def get_s3_fs():
    return s3fs.S3FileSystem(
        key=os.getenv("AWS_ACCESS_KEY_ID"),
        secret=os.getenv("AWS_SECRET_ACCESS_KEY"),
        token=os.getenv("AWS_SESSION_TOKEN", None)
    )

def validate_s3_path_exists(s3_url: str, s3: s3fs.S3FileSystem):
    if not s3_url.startswith("s3://"):
        return False, "URL must start with 's3://'"
    try:
        path = s3_url.replace("s3://", "")
        if not s3.exists(path):
            return False, f"Object not found: {s3_url}"
        return True, ""
    except Exception as e:
        return False, str(e)

def validate_s3_output_path(s3_url: str, s3: s3fs.S3FileSystem):
    if not s3_url.startswith("s3://"):
        return False, "Output path must start with 's3://'"
    try:
        bucket, *prefix_parts = s3_url.replace("s3://", "").split("/")
        prefix = "/".join(prefix_parts[:-1])
        test_path = f"{bucket}/{prefix}/.test_write_access"
        with s3.open(test_path, "w") as f:
            f.write("ok")
        s3.rm(test_path)
        return True, ""
    except Exception as e:
        return False, str(e)

