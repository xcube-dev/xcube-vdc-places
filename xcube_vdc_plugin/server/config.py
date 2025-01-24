# The MIT License (MIT)
# Copyright (c) 2024 by the xcube team and contributors
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.

from xcube.util.jsonschema import JsonArraySchema
from xcube.util.jsonschema import JsonComplexSchema
from xcube.util.jsonschema import JsonIntegerSchema
from xcube.util.jsonschema import JsonObjectSchema
from xcube.util.jsonschema import JsonStringSchema
from xcube.webapi.common.schemas import BOOLEAN_SCHEMA
from xcube.webapi.common.schemas import CHUNK_SIZE_SCHEMA
from xcube.webapi.common.schemas import FILE_SYSTEM_SCHEMA
from xcube.webapi.common.schemas import GEO_BOUNDING_BOX_SCHEMA
from xcube.webapi.common.schemas import IDENTIFIER_SCHEMA
from xcube.webapi.common.schemas import PATH_SCHEMA
from xcube.webapi.common.schemas import STRING_SCHEMA
from xcube.webapi.common.schemas import URI_SCHEMA

VARIABLES_SCHEMA = JsonArraySchema(
    items=IDENTIFIER_SCHEMA,
    min_items=1,
    description="Names of variables to be published."
    " Names may use wildcard characters '*' and '?'."
    " Also determines the order of variables.",
)

ACCESS_CONTROL_SCHEMA = JsonObjectSchema(
    properties=dict(
        IsSubstitute=BOOLEAN_SCHEMA,
        RequiredScopes=JsonArraySchema(items=IDENTIFIER_SCHEMA),
    ),
    additional_properties=False,
)

ATTRIBUTION_SCHEMA = JsonComplexSchema(
    one_of=[
        STRING_SCHEMA,
        JsonArraySchema(items=STRING_SCHEMA),
    ]
)

COMMON_VECTORDATACUBE_PROPERTIES = dict(
    Title=STRING_SCHEMA,
    Tags=JsonArraySchema(items=STRING_SCHEMA),
    Variables=VARIABLES_SCHEMA,
    BoundingBox=GEO_BOUNDING_BOX_SCHEMA,
    Hidden=BOOLEAN_SCHEMA,
    AccessControl=ACCESS_CONTROL_SCHEMA,
    Attribution=ATTRIBUTION_SCHEMA,
)

VECTORDATACUBE_SCHEMA = JsonObjectSchema(properties=dict(
    properties=dict(
        Identifier=IDENTIFIER_SCHEMA,
        StoreInstanceId=IDENTIFIER_SCHEMA,  # will be set by server
        Path=PATH_SCHEMA,
        FileSystem=FILE_SYSTEM_SCHEMA,
        Anonymous=BOOLEAN_SCHEMA,
        Endpoint=URI_SCHEMA,
        Region=IDENTIFIER_SCHEMA,
        DatasetRefs=JsonArraySchema(),
        **COMMON_VECTORDATACUBE_PROPERTIES,
    ),
    required=["Identifier", "Path"],
    additional_properties=False,
))

DATA_STORE_VECTORDATACUBE_SCHEMA = JsonObjectSchema(
    required=["Path"],
    properties=dict(
        Identifier=IDENTIFIER_SCHEMA,
        Path=PATH_SCHEMA,
        DatasetRefs=JsonArraySchema(),
        StoreInstanceId=IDENTIFIER_SCHEMA,  # will be set by server
        StoreOpenParams=JsonObjectSchema(additional_properties=True),
        **COMMON_VECTORDATACUBE_PROPERTIES,
    ),
    additional_properties=False,
)

VECTORDATACUBE_STORE_SCHEMA = JsonObjectSchema(
    properties=dict(
        Identifier=IDENTIFIER_SCHEMA,
        StoreId=IDENTIFIER_SCHEMA,
        StoreParams=JsonObjectSchema(additional_properties=True),
        Datasets=JsonArraySchema(items=DATA_STORE_VECTORDATACUBE_SCHEMA),
    ),
    required=[
        "Identifier",
        "StoreId",
    ],
    additional_properties=False,
)

VECTORDATACUBES_SCHEMA = JsonObjectSchema(properties=dict(
    VectorDataCubeStores=JsonArraySchema(items=VECTORDATACUBE_STORE_SCHEMA)
))
