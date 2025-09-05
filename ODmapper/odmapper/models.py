# Copyright (C) 2025 A*STAR

# ODmapper (Omics Data Mapping and Harmonizer) is an effort by the
# Data Management Platform in the Bioinformatics Institute (BII),
# Agency of Science, Technology and Research (A*STAR), Singapore.

# This file is part of ODmapper.

# ODmapper is an open-source tool.
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

from django.db import models
from datetime import datetime

# Authentication & Permissions
owner = models.ForeignKey('auth.User', related_name='odmapper', on_delete=models.CASCADE)
highlighted = models.TextField()

# OMOP CDM table concept
class Concept(models.Model):
    concept_id = models.IntegerField(primary_key=True)
    concept_name = models.CharField(max_length=255)
    domain_id = models.CharField(max_length=20)
    vocabulary_id = models.CharField(max_length=20)
    concept_class_id = models.CharField(max_length=20)
    standard_concept = models.CharField(max_length=1, blank=True, null=True)
    concept_code = models.CharField(max_length=50)
    valid_start_date = models.DateField()
    valid_end_date = models.DateField()
    invalid_reason = models.CharField(max_length=1, blank=True, null=True)

    def __str__ (self):
        return f'{self.concept_id}'

    class Meta:
        managed = False
        db_table = 'concept'

class ConceptSynonym(models.Model):
    concept_id = models.ForeignKey(Concept, on_delete=models.CASCADE, db_column='concept_id')
    concept_synonym_name = models.CharField(max_length=1000, primary_key=True)
    language_concept_id = models.IntegerField()

    class Meta:
        managed = False
        db_table = 'concept_synonym'

class ConceptRelationship (models.Model):
    concept_id_1 = models.ForeignKey(Concept, on_delete=models.CASCADE, db_column='concept_id_1', related_name='maps_from')
    concept_id_2 = models.ForeignKey(Concept, on_delete=models.CASCADE, db_column='concept_id_2', related_name='maps_to')
    relationship_id = models.CharField(max_length=50, primary_key=True)

    class Meta:
        managed = False
        db_table = 'concept_relationship'
