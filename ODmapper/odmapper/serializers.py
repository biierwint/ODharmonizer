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

from rest_framework import serializers
from odmapper.models import Concept, ConceptSynonym
from django.contrib.auth.models import User

class ConceptSerializer (serializers.ModelSerializer):
    class Meta:
        model = Concept
        fields = '__all__'
        #fields = ['concept_id', 'concept_name', 'concept_code']

class ConceptSynonymSerializer (serializers.ModelSerializer):
    concepts = ConceptSerializer (read_only=True, source='concept_id')

    class Meta:
        model = ConceptSynonym
        fields = ['concept_id', 'concept_synonym_name', 'language_concept_id', 'concepts']
        #fields = '__all__'

