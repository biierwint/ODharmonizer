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
This script is part of the ODharmonizer-v1.0 > ODannotator
The purpose of this script is to get annotate variants with OMOP concept_ids.
This script has been added as part of vrs-annotate.

Example usage: vrs-annotate omop --help

'''
import logging
import pickle
from enum import Enum
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed, wait
from tqdm import tqdm
import requests
from requests.adapters import HTTPAdapter, Retry
import pysam

# Build a global session with retry/backoff
_session = requests.Session()
retries = Retry(
    total=5,                # retry up to 5 times
    backoff_factor=0.5,     # wait 0.5s, 1s, 2s, 4s...
    status_forcelist=[500, 502, 503, 504],  # retry only on these
    raise_on_status=False,
)
_session.mount("http://", HTTPAdapter(max_retries=retries))
_session.mount("https://", HTTPAdapter(max_retries=retries))

_logger = logging.getLogger(__name__)


class OMOPAnnotatorError(Exception):
    """Custom exceptions for VCF Annotator tool"""


class FieldName(str, Enum):
    IDS_FIELD = "VRS_Allele_IDs"
    OMOP_CONCEPT_ID_FIELD = "OMOP_Concept_IDs"
    ERROR_FIELD = "OMOP_Error"


VCF_ESCAPE_MAP = str.maketrans(
    {
        "%": "%25",
        ";": "%3B",
        ",": "%2C",
        "\r": "%0D",
        "\n": "%0A",
    }
)


class OMOPAnnotator:
    def __init__(self, odmapper_base_url) -> None:
        self.odmapper_base_url = odmapper_base_url

    def _update_vcf_header(self, vcf: pysam.VariantFile) -> None:
        vcf.header.info.add(
            FieldName.OMOP_CONCEPT_ID_FIELD.value,
            "R",  # REF and ALT alleles
            "String",
            (
                "The mapped OMOP Genomic concept_id based on the VRS computed identifiers "
                "corresponding to the GT indexes of the REF and ALT alleles"
            ),
        )
        vcf.header.info.add(
            FieldName.ERROR_FIELD.value,
            ".",
            "String",
            "If an error occurred during mapping to OMOP concept id, the error message",
        )

    def annotate(
        self,
        input_vcf_path: Path,
        output_vcf_path: Path | None = None,
        output_pkl_path: Path | None = None,
        odmapper_base_url: str | None = None,
    ) -> None:
    
        if not any((output_vcf_path, output_pkl_path)):
            raise OMOPAnnotatorError(
                "Must provide one of: `output_vcf_path` or `output_pkl_path`"
            )
    
        vcf = pysam.VariantFile(str(input_vcf_path.absolute()))
        if output_vcf_path:
            self._update_vcf_header(vcf)
            vcf_out = pysam.VariantFile(str(output_vcf_path.absolute()), "w", header=vcf.header)
        else:
            vcf_out = None
    
        omop_data = {} if output_pkl_path else None
    
        max_workers = 8
        executor = ThreadPoolExecutor(max_workers=max_workers)
        futures = dict()
        pbar = tqdm(desc="Annotating VCF", unit=" records")
    
        def process_record(record_index, record):
            additional_info_fields = [FieldName.OMOP_CONCEPT_ID_FIELD] if vcf_out else []
            try:
                omop_field_data = self._get_omop_data(
                    record,
                    omop_data,
                    additional_info_fields,
                    odmapper_base_url or self.odmapper_base_url,
                )
            except Exception as ex:
                _logger.exception("OMOP mapping error on %s-%s", record.chrom, record.pos)
                err_msg = f"{ex}" or f"{type(ex)}"
                err_msg = err_msg.translate(VCF_ESCAPE_MAP)
                additional_info_fields = [FieldName.ERROR_FIELD]
                omop_field_data = {FieldName.ERROR_FIELD.value: [err_msg]}
            return record_index, record, omop_field_data, additional_info_fields
    
        # Submit tasks with index
        for idx, record in enumerate(vcf):
            futures[idx] = executor.submit(process_record, idx, record)
    
        # Collect results in a dict keyed by index
        results = {}
        for future in tqdm(as_completed(futures.values()), total=len(futures), desc="Processing"):
            idx, record, omop_field_data, info_fields = future.result()
            results[idx] = (record, omop_field_data, info_fields)
            pbar.update(1)
    
        # Write output in original order
        for idx in range(len(results)):
            record, omop_field_data, info_fields = results[idx]
            if vcf_out:
                for k in info_fields:
                    record.info[k.value] = [
                        str(x) if x is not None else "-" for x in omop_field_data[k.value]
                    ]
                vcf_out.write(record)
    
        pbar.close()
        executor.shutdown()
        vcf.close()
    
        if vcf_out:
            vcf_out.close()
    
        if output_pkl_path:
            with output_pkl_path.open("wb") as wf:
                pickle.dump(omop_data, wf)
    
        
    def _get_omop_data(
        self,
        record: pysam.VariantRecord,
        omop_data: dict | None,
        additional_info_fields: list[FieldName],
        odmapper_base_url,
    ) -> dict:

        omop_field_data = {field.value: [] for field in additional_info_fields}
        vrs_ids = record.info.get("VRS_Allele_IDs", [])
        record_id = f"{record.id}"

        def fetch_concept(vrs_id):
            if vrs_id == "-":
                _logger.debug("Unknown VRS ID found: %s", record_id)
                return vrs_id, "-"
            try:
                url = f"{odmapper_base_url}/synonym/{vrs_id}/"
                response = _session.get(url, verify=False, timeout=10)
                if response.status_code == 200:
                    data = response.json()
                    concept_id = data.get("concept_id") if data else "-"
                    return vrs_id, concept_id or "-"
                else:
                    _logger.warning("Non-200 response %s for %s", response.status_code, vrs_id)
                    return vrs_id, "-"
            except Exception as e:
                _logger.warning("Error fetching OMOP concept for %s: %s", vrs_id, str(e))
                return vrs_id, "-"

        max_workers = min(8, len(vrs_ids)) if vrs_ids else 1
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            results = list(executor.map(fetch_concept, vrs_ids))
            for _, concept_id in results:
                omop_field_data[FieldName.OMOP_CONCEPT_ID_FIELD].append(concept_id)

        return omop_field_data

