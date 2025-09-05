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

from django.urls import path, include
from django.conf import settings
from django.conf.urls.static import static
from django.contrib import admin

from drf_spectacular.views import SpectacularAPIView
from drf_spectacular.views import SpectacularSwaggerView

from odmapper import views
from rest_framework import permissions

urlpatterns = [
    path('api/schema/', SpectacularAPIView.as_view(urlconf=[
        path('api/odmapper/concept_id/<int:concept_id>/', views.ConceptDetail.as_view()),
        path('api/odmapper/query/<str:query_text>/', views.ConceptSeekList.as_view()),
        path('api/odmapper/synonym/<str:synonym>/', views.ConceptSynonymList.as_view()),
        path('api/odmapper/<str:feature>/<str:vocabulary>/<str:concept_code>/', views.ConceptLookupView.as_view()),
        path('api/odmapper/standard/<int:concept_id>/', views.ConceptStandardView.as_view()),
    ]), name='schema'),
    path('api/schema/swagger-ui/', SpectacularSwaggerView.as_view(url_name='schema'), name='swagger-ui'),
    path('api/odmapper/concept_id/<int:concept_id>/', views.ConceptDetail.as_view()),
    path('api/odmapper/query/<str:query_text>/', views.ConceptSeekList.as_view()),
    path('api/odmapper/synonym/<str:synonym>/', views.ConceptSynonymList.as_view()),
    path('api/odmapper/<str:feature>/<str:vocabulary>/<str:concept_code>/', views.ConceptLookupView.as_view()),
    path('api/odmapper/standard/<int:concept_id>/', views.ConceptStandardView.as_view(), name='concept-standard'),
    path('', views.ConceptQuery.as_view(), name='index'),
]

urlpatterns += [
    path('api-auth/', include('rest_framework.urls')),
] + static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)
