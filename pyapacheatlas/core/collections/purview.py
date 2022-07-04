import enum
import logging
from typing import List, Union

from ..entity import AtlasEntity
from ..util import _handle_response, AtlasBaseClient, batch_dependent_entities

import requests

#me
from anytree import Node, RenderTree, AsciiStyle


class PurviewCollectionsClient(AtlasBaseClient):
    """
    Some support for purview collections api.

    See also:
    https://docs.microsoft.com/en-us/rest/api/purview/catalogdataplane/collection

    """
    def __init__(self, endpoint_url : str, authentication, **kwargs):
        """
        :param str endpoint_url:
            Base URL for purview account, e.g. "https://{account}.purview.azure.com/" .
        """
        super().__init__(**kwargs)
        self.endpoint_url = endpoint_url
        self.authentication = authentication

    def upload_single_entity(
        self,
        entity : Union[AtlasEntity, dict],
        collection : str,
        api_version : str = "2022-03-01-preview"
    ):
        """
        Creates or updates a single atlas entity in a purview collection.

        See also
        https://docs.microsoft.com/en-us/rest/api/purview/catalogdataplane/collection/create-or-update

        :param Union[AtlasEntity, dict] entity: The entity to create or update.
        :param str collection:
            Collection ID of the containing purview collection.
            Typically a 6-letter pseudo-random string such as "xcgw8s" which can be obtained
            e.g. by visual inspection in the purview web UI (https://web.purview.azure.com/).
        :param str api_version: The Purview API version to use.
        :return: An entity mutation response.
        :rtype: dict
        """

        atlas_endpoint = self.endpoint_url + f"catalog/api/collections/{collection}/entity"

        if isinstance(entity, AtlasEntity):
            payload = {"entity":entity.to_json(), "referredEntities":{}}
        elif isinstance(entity, dict):
            payload = entity
        else:
            raise ValueError("entity should be an AtlasEntity or dict")

        singleEntityResponse = requests.post(
            atlas_endpoint,
            json = payload,
            params = {"api-version": api_version},
            headers = self.authentication.get_authentication_headers(),
            **self._requests_args
        )
        results = _handle_response(singleEntityResponse)
        return results

    def upload_entities(
        self,
        batch : List[AtlasEntity],
        collection : str,
        batch_size : int = None,
        api_version : str = "2022-03-01-preview"
    ):
        """
        Creates or updates a batch of atlas entities in a purview collection.

        See also
        https://docs.microsoft.com/en-us/rest/api/purview/catalogdataplane/collection/create-or-update-bulk

        :param batch:
            The batch of entities you want to upload. Supports a single dict,
            AtlasEntity, list of dicts, list of atlas entities.
        :type batch:
            Union(dict, :class:`~pyapacheatlas.core.entity.AtlasEntity`,
            list(dict), list(:class:`~pyapacheatlas.core.entity.AtlasEntity`) )
        :param str collection:
            Collection ID of the containing purview collection.
            Typically a 6-letter pseudo-random string such as "xcgw8s" which can be obtained
            e.g. by visual inspection in the purview web UI (https://web.purview.azure.com/).
        :param int batch_size: The number of entities you want to send in bulk.
        :param str api_version: The Purview API version to use.
        :return: An entity mutation response.
        :rtype: dict
        """

        atlas_endpoint = self.endpoint_url + f"catalog/api/collections/{collection}/entity/bulk"

        payload = PurviewCollectionsClient._prepare_entity_upload(batch)
        results = []
        if batch_size and len(payload["entities"]) > batch_size:
            batches = [{"entities": x} for x in batch_dependent_entities(
                payload["entities"], batch_size=batch_size)]

            for batch_id, batch in enumerate(batches):
                batch_size = len(batch["entities"])
                logging.debug(f"Batch upload #{batch_id} of size {batch_size}")
                postBulkEntities = requests.post(
                    atlas_endpoint,
                    json=batch,
                    params = {"api-version": api_version},
                    headers=self.authentication.get_authentication_headers(),
                    **self._requests_args
                )
                temp_results = _handle_response(postBulkEntities)
                results.append(temp_results)

        else:
            postBulkEntities = requests.post(
                atlas_endpoint,
                json=payload,
                params = {"api-version": api_version},
                headers=self.authentication.get_authentication_headers(),
                **self._requests_args
            )

            results = _handle_response(postBulkEntities)

        return results
    
    # TODO: This is duplication with the AtlasClient and should eventually be removed
    @staticmethod
    def _prepare_entity_upload(batch):
        """
        Massages the batch to be in the right format and coerces to json/dict.
        Supports list of dicts, dict of single entity, dict of AtlasEntitiesWithExtInfo.

        :param batch: The batch of entities you want to upload.
        :type batch: Union(list(dict), dict))
        :return: Provides a dict formatted in the Atlas entity bulk upload.
        :rtype: dict(str, list(dict))
        """
        payload = batch
        required_keys = ["entities"]

        if isinstance(batch, list):
            # It's a list, so we're assuming it's a list of entities
            # Handles any type of AtlasEntity and mixed batches of dicts
            # and AtlasEntities
            dict_batch = [e.to_json() if isinstance(
                e, AtlasEntity) else e for e in batch]
            payload = {"entities": dict_batch}
        elif isinstance(batch, dict):
            current_keys = list(batch.keys())

            # Does the dict entity conform to the required pattern?
            if not any([req in current_keys for req in required_keys]):
                # Assuming this is a single entity
                # DESIGN DECISION: I'm assuming, if you're passing in
                # json, you know the schema and I will not support
                # AtlasEntity here.
                payload = {"entities": [batch]}
        elif isinstance(batch, AtlasEntity):
            payload = {"entities": [batch.to_json()]}
        else:
            raise NotImplementedError(
                f"Uploading type: {type(batch)} is not supported.")

        return payload

    def move_entities(
        self,
        guids : List[str],
        collection : str,
        api_version : str = "2022-03-01-preview"
    ):
        """
        Move one or more entities based on their guid to the provided collection.

        See also
        https://docs.microsoft.com/en-us/rest/api/purview/catalogdataplane/collection/move-entities-to-collection

        :param str collection:
            Collection ID of the containing purview collection.
            Typically a 6-letter pseudo-random string such as "xcgw8s" which can be obtained
            e.g. by visual inspection in the purview web UI (https://web.purview.azure.com/).
        :param str api_version: The Purview API version to use.
        :return: An entity mutation response.
        :rtype: dict
        """

        atlas_endpoint = self.endpoint_url + f"catalog/api/collections/{collection}/entity/moveHere"

        singleEntityResponse = requests.post(
            atlas_endpoint,
            json = {"entityGuids":guids},
            params = {"api-version": api_version},
            headers = self.authentication.get_authentication_headers(),
            **self._requests_args
        )
        results = _handle_response(singleEntityResponse)
        return results

    def _list_collections_generator(self, initial_endpoint):
        """
        Generator to page through the list collections response
        """
        updated_endpoint = initial_endpoint
        while True:
            if updated_endpoint is None:
                return
            collectionsListGet = requests.get(
                updated_endpoint,
                headers = self.authentication.get_authentication_headers(),
                **self._requests_args
            )

            results = _handle_response(collectionsListGet)

            return_values = results["value"]
            return_count = len(return_values)
            updated_endpoint = results.get("nextLink")

            if return_count == 0:
                return

            for sub_result in return_values:
                try:
                    yield sub_result
                except StopIteration:
                    return

    def list_collections(
        self,
        api_version : str = "2019-11-01-preview",
        skipToken : str = None
    ):
        """
        List the collections in the account.

        :param str api_version: The Purview API version to use.
        :param str skipToken: It is unclear at this time what values would be
            provided to this parameter.
        :return: A generator that pages through the list collections
        :rtype: List[dict]
        """
        atlas_endpoint = self.endpoint_url + f"collections?api-version={api_version}"
        if skipToken:
            atlas_endpoint = atlas_endpoint + f"&$skipToken={skipToken}"

        collection_generator = self._list_collections_generator(atlas_endpoint)

        return collection_generator






    #me


    def list_collections_new(
        self,
        api_version: str = "2019-11-01-preview",
        hierarchy: bool = False,
        only_names: bool = False,
        skipToken: str = None
    ):

        """list collectons and a couple things to it.
        """

        atlas_endpoint = self.endpoint_url + f"collections?api-version={api_version}"
        if skipToken:
            atlas_endpoint = atlas_endpoint + f"&$skipToken={skipToken}"
        
        collection_list_request = requests.get(
            url=atlas_endpoint,
            headers=self.authentication.get_authentication_headers()
        )
        collection_list = collection_list_request.json()["value"]
        if hierarchy:
            return self._hierarchy()

        elif only_names:
            friendly_names_list = [coll["friendlyName"] for coll in collection_list]
            return friendly_names_list

        return collection_list

    def _get_final_collection_names(self):
        collection_list = self.list_collections_new()

        final_collection_list = {}
        for index, coll in enumerate(collection_list):
            if "parentCollection" not in coll:
                final_collection_list[coll["name"]] = {
                    "friendlyName": coll["friendlyName"], 
                    "index": index, 
                    "parentCollection": None
                }
            else:
                final_collection_list[coll["name"]] = {
                    "friendlyName": coll["friendlyName"], 
                    "index": index, 
                    "parentCollection": coll['parentCollection']['referenceName']
                }
        return final_collection_list

    
    def _hierarchy(self):
        collection_list = self._get_final_collection_names()
        for index, coll in enumerate(collection_list.items()):
            real_name = coll[0]
            friendly_name = coll[1]["friendlyName"]
            parent_collection = coll[1]["parentCollection"]

            if index == 0:
                root = Node(name=friendly_name)
                root_name = real_name 
            
            elif index != 0 and parent_collection == root_name:
                child = Node(name=friendly_name, parent=root)
            else:
                child = Node(name=friendly_name, parent=child)
        
        for pre, _, node in RenderTree(node=root, style=AsciiStyle()):
            print("%s%s" % (pre, node.name))




    def create_collectiions(
        self, 
        collection_to_start_on: str,
        collection_names: list[str] = None,
        api_version: str = "2019-11-01-preview"
    ):

        collection_list = self._get_final_collection_names()

        collection_to_start_on = collection_to_start_on.strip()
        collection_to_start_on_check = []
        for name, value in collection_list.items():
            if collection_to_start_on == value["friendlyName"] or collection_to_start_on == name:
                collection_to_start_on = name 
                collection_to_start_on_check.append(collection_to_start_on)
        
        if not collection_to_start_on_check:
            raise ValueError(f"The collection '{collection_to_start_on}' either doesn't exist or you don\'t have permission to start on it. Would need to be a collection admin on that collection if it exists.")

        for name in collection_names:
            if "/" in name:
                split_names = name.split("/")
                final_names = [name.strip() for name in split_names]
            else:
                final_names = [name.strip()]
            

            updated_final_names = final_names.copy()
            for index, name in enumerate(final_names):
                for coll, value in collection_list.items():
                    if name == value["friendlyName"] and name != coll and ' ' in name:
                        updated_final_names[index] = value["friendlyName"]
                    elif name == value["friendlyName"] and name != coll:
                        updated_final_names[index] = coll
            
            print(updated_final_names)
            for index, name in enumerate(updated_final_names, start=1):
                if ' ' in name:
                    friendly_name =  name
                    name = name.replace(" ", "")
                else:
                    friendly_name = name
                     
                atlas_endpoint = self.endpoint_url + f"account/collections/{name}?api-version={api_version}"
                headers=self.authentication.get_authentication_headers()
                if index == 1:
                    data = f'{{"parentCollection": {{"referenceName": "{collection_to_start_on}"}}, "friendlyName": "{final_names[index - 1]}"}}'
                    print(data, index)
                    request = requests.put(url=atlas_endpoint, headers=headers, data=data)
                    print(request.content, '\n')
                else:
                    data = f'{{"parentCollection": {{"referenceName": "{updated_final_names[index - 2]}"}}, "friendlyName": "{friendly_name}"}}'
                    print(data, index)
                    request = requests.put(url=atlas_endpoint, headers=headers, data=data)
                    print(request.content, '\n')
 

    def create_collectiions2(
        self, 
        collection_to_start_on: str,
        collection_names: list[str] = None,
        api_version: str = "2019-11-01-preview"
    ):

        collection_list = self._get_final_collection_names()

        collection_to_start_on = collection_to_start_on.strip()
        collection_to_start_on_check = []
        for name, value in collection_list.items():
            if collection_to_start_on == value["friendlyName"] or collection_to_start_on == name:
                collection_to_start_on = name 
                collection_to_start_on_check.append(collection_to_start_on)
        
        if not collection_to_start_on_check:
            raise ValueError(f"The collection '{collection_to_start_on}' either doesn't exist or you don\'t have permission to start on it. Would need to be a collection admin on that collection if it exists.")

        for name in collection_names:
            if "/" in name:
                split_names = name.split("/")
                final_names = [name.strip() for name in split_names]
            else:
                final_names = [name.strip()]
  
            collection_dict = {}
            for index, name in enumerate(final_names):
                for coll, value in collection_list.items():

                    if index == 0 and ' ' in name:
                        friendly_name = name 
                        updated_name = name.replace(" ", "")
                        collection_dict[updated_name] = {"friendlyName": friendly_name, "parentCollection": collection_to_start_on}
                    elif index == 0:
                        collection_dict[name] = {"friendlyName": name, "parentCollection": collection_to_start_on}

                    elif name == value["friendlyName"] and name != coll and index > 0:
                        collection_dict[coll] = {"friendlyName": name, "parentCollection": value["parentCollection"]}
                    elif name == coll and index > 0:
                        collection_dict[name] = {"friendlyName": name, "parentCollection": value["parentCollection"]}
                    
                    elif name != value["friendlyName"] and name != coll and ' ' in name and index > 0:
                        friendly_name = name 
                        updated_name = name.replace(" ", "")
                        collection_dict[updated_name] = {"friendlyName": friendly_name, "parentCollection": None}
                    else:
                        collection_dict[name] = {"friendlyName": name, "parentCollection": None}


            collection_updated_list = list(collection_dict.items())

            really_final_list = []
            for index, value in enumerate(collection_updated_list):
                if value[1]["parentCollection"] is None:
                    new_key = [value[0], value[1]["friendlyName"], collection_updated_list[index - 1][0]]
                    really_final_list.append(new_key)
                else:
                    really_final_list.append([value[0], value[1]["friendlyName"], value[1]["parentCollection"]])
            
            for item in really_final_list:
                atlas_endpoint = self.endpoint_url + f"account/collections/{item[0]}?api-version={api_version}"
                headers=self.authentication.get_authentication_headers()
                data = f'{{"parentCollection": {{"referenceName": "{item[2]}"}}, "friendlyName": "{item[1]}"}}'
                request = requests.put(url=atlas_endpoint, headers=headers, data=data)
                print(request.content, '\n')





            # updated_final_names = final_names.copy()
            # for index, name in enumerate(final_names):
            #     for coll, value in collection_list.items():
            #         if name == value["friendlyName"] and name != coll and ' ' in name:
            #             updated_final_names[index] = value["friendlyName"]
            #         elif name == value["friendlyName"] and name != coll:
            #             updated_final_names[index] = coll
            
            # print(updated_final_names)
            # for index, name in enumerate(updated_final_names, start=1):
            #     if ' ' in name:
            #         friendly_name =  name
            #         name = name.replace(" ", "")
            #     else:
            #         friendly_name = name
                     
            #     atlas_endpoint = self.endpoint_url + f"account/collections/{name}?api-version={api_version}"
            #     headers=self.authentication.get_authentication_headers()
            #     if index == 1:
            #         data = f'{{"parentCollection": {{"referenceName": "{collection_to_start_on}"}}, "friendlyName": "{final_names[index - 1]}"}}'
            #         print(data, index)
            #         request = requests.put(url=atlas_endpoint, headers=headers, data=data)
            #         print(request.content, '\n')
            #     else:
            #         data = f'{{"parentCollection": {{"referenceName": "{updated_final_names[index - 2]}"}}, "friendlyName": "{friendly_name}"}}'
            #         print(data, index)
            #         request = requests.put(url=atlas_endpoint, headers=headers, data=data)
            #         print(request.content, '\n')



