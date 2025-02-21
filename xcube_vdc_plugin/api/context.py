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
import datetime
import fnmatch
import itertools
import json
import os
import re
from typing import Mapping, Any, Optional, List, Dict, Hashable
import dateutil.parser
from geopandas import GeoDataFrame

from xcube.constants import LOG
from xcube.core.store import DataStorePool
from xcube.core.store import VECTOR_DATA_CUBE_TYPE
from xcube.server.api import ApiContext, ApiError
from xcube.server.api import Context
from xcube.server.config import is_absolute_path
from xcube.core.store import DataStoreConfig
from xcube.webapi.places import PlacesContext
from xcube.webapi.places.context import PlaceGroup
from xcube.util.frozen import Frozen
from xcube.util.frozen import FrozenDict

ServerConfig = FrozenDict[str, Any]

STORE_DS_ID_SEPARATOR = "~"

VdcConfig = Mapping[str, Any]


def _is_wildcard(string: str) -> bool:
    return "?" in string or "*" in string


def _get_selected_dataset_config(
    store_dataset_id: str, store_instance_id: str, dataset_config_base: dict
) -> dict:
    LOG.debug(f"Selected dataset {store_dataset_id!r}")
    dataset_config = dict(StoreInstanceId=store_instance_id, **dataset_config_base)
    if "Identifier" in dataset_config and dataset_config["Path"] != store_dataset_id:
        raise ApiError.InvalidServerConfig(
            "User-defined identifiers can only be assigned"
            " to datasets with non-wildcard paths."
        )
    elif "Identifier" not in dataset_config:
        dataset_config["Path"] = store_dataset_id
        dataset_config["Identifier"] = (
            f"{store_instance_id}{STORE_DS_ID_SEPARATOR}{store_dataset_id}"
        )
    return dataset_config


class VdcPlacesPluginContext(ApiContext):

    def __init__(self, server_ctx: Context):
        super().__init__(server_ctx)
        self._places_ctx: PlacesContext = server_ctx.get_api_ctx("places")
        self.config = dict(server_ctx.config)
        self.root = server_ctx
        self._data_store_pool, self._vdc_configs = self._process_dataset_configs(
            self.config
        )

    @classmethod
    def _process_dataset_configs(
        cls, config: ServerConfig
    ) -> tuple[DataStorePool, list[dict[str, Any]]]:
        data_store_configs = config.get("VectorDataCubeStores", [])

        data_store_pool = DataStorePool()
        for data_store_config_dict in data_store_configs:
            store_instance_id = data_store_config_dict.get("Identifier")
            store_id = data_store_config_dict.get("StoreId")
            store_params = data_store_config_dict.get("StoreParams", {})
            store_dataset_configs = data_store_config_dict.get("Datasets")
            store_config = DataStoreConfig(
                store_id, store_params=store_params, user_data=store_dataset_configs
            )
            data_store_pool.add_store_config(store_instance_id, store_config)
        dataset_configs = cls.get_dataset_configs_from_stores(
            data_store_pool
        )
        dataset_configs = [dict(c) for c in dataset_configs]
        return data_store_pool, dataset_configs

    @classmethod
    def get_dataset_configs_from_stores(
        cls, data_store_pool: DataStorePool
    ) -> list[VdcConfig]:
        all_dataset_configs: list[VdcConfig] = []
        for store_instance_id in data_store_pool.store_instance_ids:
            LOG.info(f"Scanning store {store_instance_id!r}")
            data_store_config = data_store_pool.get_store_config(store_instance_id)

            # Note by forman: This iterator chaining is inefficient.
            # Preferably, we should offer
            #
            # store_dataset_ids = data_store.get_data_ids(
            #     data_type=(DATASET_TYPE, MULTI_LEVEL_DATASET_TYPE)
            # )
            #

            store_dataset_configs: list[ServerConfig] = data_store_config.user_data
            if store_dataset_configs:
                for store_dataset_config in store_dataset_configs:
                    dataset_id_pattern = store_dataset_config.get("Path", "*")
                    if _is_wildcard(dataset_id_pattern):
                        data_store = data_store_pool.get_store(store_instance_id)
                        store_dataset_ids = itertools.chain(
                            data_store.get_data_ids(data_type=VECTOR_DATA_CUBE_TYPE)
                        )
                        for store_dataset_id in store_dataset_ids:
                            if fnmatch.fnmatch(store_dataset_id, dataset_id_pattern):
                                all_dataset_configs.append(
                                    _get_selected_dataset_config(
                                        store_dataset_id,
                                        store_instance_id,
                                        store_dataset_config,
                                    )
                                )
                    else:
                        all_dataset_configs.append(
                            _get_selected_dataset_config(
                                store_dataset_config["Path"],
                                store_instance_id,
                                store_dataset_config,
                            )
                        )
        return all_dataset_configs

    @property
    def config(self) -> Mapping[str, Any]:
        assert self._config is not None
        return self._config

    @config.setter
    def config(self, config: Mapping[str, Any]):
        assert isinstance(config, Mapping)
        self._config = dict(config)

    def on_update(self, prev_context: Optional["Context"]):
        if prev_context:
            self.config = prev_context.config
        self.update_places()

    def update_places(self):
        if len(self._vdc_configs) == 0:
            return
        LOG.debug('Reading in Vector Data Cubes')
        gdfs = self._read_vector_datacubes_as_geodataframes()
        LOG.debug('Finished reading Vector Data Cubes.')

        LOG.debug('Adding Vector Data Cube Place Groups')
        for gdf in gdfs:
            place_group_config: Dict[Hashable, Any] = dict()
            for k in gdf.attrs.keys():
                place_group_config[k] = gdf.attrs[k]
            place_group = self._create_place_group(place_group_config, gdf)
            dataset_ids = place_group_config.get('DatasetRefs', [])
            self._places_ctx.add_place_group(place_group, dataset_ids)
        LOG.debug('Finished adding Vector Data Cube Place Groups.')

    def _create_place_group(self,
                            place_group_config: Dict[Hashable, Any],
                            gdf: GeoDataFrame) -> PlaceGroup:
        place_group_id = place_group_config.get("PlaceGroupRef")
        if place_group_id:
            raise ApiError.InvalidServerConfig(
                "'PlaceGroupRef' cannot be used in a GDF place group"
            )
        place_group_id = self._places_ctx.get_place_group_id_safe(place_group_config)

        place_group = self._places_ctx.get_cached_place_group(place_group_id)
        if place_group is None:
            place_group_title = place_group_config.get("Title", place_group_id)
            base_url = f'http://{self.root.config["address"]}:' \
                       f'{self.root.config["port"]}'
            property_mapping = self._places_ctx.get_property_mapping(
                base_url, place_group_config
            )
            source_encoding = place_group_config.get("CharacterEncoding",
                                                     "utf-8")
            place_group = dict(type="FeatureCollection",
                               features=None,
                               id=place_group_id,
                               title=place_group_title,
                               propertyMapping=property_mapping,
                               sourcePaths='None',
                               sourceEncoding=source_encoding)

            self._places_ctx.check_sub_group_configs(place_group_config)
            self._places_ctx.set_cached_place_group(place_group_id, place_group)

        self.load_gdf_place_group_features(place_group, gdf)
        return place_group

    @staticmethod
    def load_gdf_place_group_features(
            place_group: PlaceGroup, gdf: GeoDataFrame) -> None:
        features = place_group.get('features')
        if features is not None:
            return features
        feature_collection = json.loads(gdf.to_json())
        for feature in feature_collection['features']:
            VdcPlacesPluginContext._clean_time_name(feature['properties'])
        place_group['features'] = feature_collection['features']

    def _read_vector_datacubes_as_geodataframes(self) -> List[GeoDataFrame]:
        gdfs = []
        for vdc_config in self._vdc_configs:
            if isinstance(vdc_config, Frozen):
                vdc_config = vdc_config.defrost()
            for k, v in vdc_config.items():
                if isinstance(v, Frozen):
                    vdc_config[k] = v.defrost()
            vdc_id: str = vdc_config.get("Identifier")
            store_instance_id = vdc_config.get("StoreInstanceId")
            data_store_pool = self._data_store_pool
            data_store = data_store_pool.get_store(store_instance_id)
            data_id = vdc_config.get("Path")
            open_params = dict(vdc_config.get("StoreOpenParams") or {})
            # open_params_schema = data_store.get_open_data_params_schema(data_id=data_id)
            data_opener_ids = data_store.get_data_opener_ids(data_id)
            vdc = None
            for data_opener_id in data_opener_ids:
                if data_opener_id.startswith("vectordatacube"):
                    vdc = data_store.open_data(
                        data_id,
                        opener_id=data_opener_id,
                        **open_params
                    )
                    break
            if vdc is None:
                LOG.debug('Could not find vector data cube opener')
                continue
            if vdc_config.get("Split", False):
                for j, coord in enumerate(vdc.xvec.geom_coords):
                    geometry_name = coord
                    break
                label_coord = vdc_config.get("LabelCoord", geometry_name)
                labels = vdc[label_coord].values
                for i, label in enumerate(labels):
                    sub_vdc = vdc.isel({geometry_name: i})
                    sub_gdf = sub_vdc.xvec.to_geodataframe(geometry=geometry_name)
                    LOG.debug("Created sub-geodataframe")
                    for k in vdc_config.keys():
                        sub_gdf.attrs[k] = vdc_config[k]
                    extension = label if label_coord is not geometry_name else i + 1
                    sub_gdf.attrs["Title"] = f"{sub_gdf.attrs['Title']} - {extension}"
                    sub_gdf.attrs["Identifier"] = f"{sub_gdf.attrs['Identifier']}_{extension}"
                    gdfs.append(sub_gdf)
                    LOG.debug(f"Appended {sub_gdf.attrs['Title']}")
            else:
                gdf = vdc.xvec.to_geodataframe()
                for k in vdc_config.keys():
                    gdf.attrs[k] = vdc_config[k]
                gdfs.append(gdf)
                LOG.debug(f"Appended {gdf.attrs['Title']}")
        return gdfs

    @staticmethod
    def _clean_time_name(properties: Dict):
        illegal_names = ['datetime', 'timestamp', 'date-time', 'date']
        for n in illegal_names:
            if n in properties:
                properties['time'] = dateutil.parser.parse(
                    properties[n]).isoformat()
                del properties[n]
