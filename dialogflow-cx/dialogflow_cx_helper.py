#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ***************************************************************************80
#
# python delete_and_rename_intents.py
#
# *****************************************************************************

# standard imports
import json
from os import listdir
import urllib.parse
import os
from os.path import isfile, join, exists
import re

# third Party imports
import pandas
from google.api_core.client_options import ClientOptions
from google.cloud import dialogflowcx_v3
from google.protobuf import field_mask_pb2


def rename_intents(location: str, intent: dialogflowcx_v3.Intent) -> dialogflowcx_v3.Intent:
    """Renames the intent provided

    Parameter
    ---------
    location: str
        location of the agent
    intent: dialogflowcx_v3.Intent
        entire intent object

    Returns
    -------
    intent object with new display name
    """

    client = dialogflowcx_v3.IntentsClient(client_options=ClientOptions(api_endpoint=f"{location}-dialogflow.googleapis.com"))

    # Initialize request argument(s)
    request = dialogflowcx_v3.UpdateIntentRequest(
        intent=intent,
        update_mask=field_mask_pb2.FieldMask(paths=["display_name"])
    )
    # Make the request
    response = client.update_intent(request=request)
    return response


def list_pages(location: str, flow_name: str) -> dialogflowcx_v3.ListPagesResponse:
    """List all the pages defined in the flow

    Parameter
    ---------
    location: str
        location of the agent
    flow_name: str
        Format: projects/<project-id>/locations/<location>/agents/<agent-id>/flows/<flow-id>

    Returns
    -------
    List of all the pages
    """

    # Create a client
    client = dialogflowcx_v3.PagesClient(client_options=ClientOptions(api_endpoint=f"{location}-dialogflow.googleapis.com"))

    # Initialize request argument(s)
    request = dialogflowcx_v3.ListPagesRequest(
        parent=flow_name,
    )

    # Make the request
    page_result = client.list_pages(request=request)

    return page_result


def list_flows(location: str, agent_name: str) -> dialogflowcx_v3.ListFlowsResponse:
    """List all the flows defined in the agent

    Parameter
    ---------
    location: str
        location of the agent
    agent_name: str
        Format: projects/<project-id>/locations/<location>/agents/<agent-id>

    Returns
    -------
    List of all the flows
    """

    client = dialogflowcx_v3.FlowsClient(client_options=ClientOptions(api_endpoint=f"{location}-dialogflow.googleapis.com"))

    # Initialize request argument(s)
    request = dialogflowcx_v3.ListFlowsRequest(
        parent=agent_name
    )

    # Make the request
    response = client.list_flows(request=request)
    return response


def list_transition_route_groups(location: str, flow_name: str) -> dialogflowcx_v3.ListTransitionRouteGroupsResponse:
    """List all the pages defined in the flow

    Parameter
    ---------
    location: str
        location of the agent
    flow_name: str
        Format: projects/<project-id>/locations/<location>/agents/<agent-id>/flows/<flow-id>

    Returns
    -------
    List of all the pages
    """

    client = dialogflowcx_v3.TransitionRouteGroupsClient(client_options=ClientOptions(api_endpoint=f"{location}-dialogflow.googleapis.com"))

    # Initialize request argument(s)
    request = dialogflowcx_v3.ListTransitionRouteGroupsRequest(
        parent=flow_name
    )

    # Make the request
    response = client.list_transition_route_groups(request=request)
    return response


def delete_intent(location: str, full_intent_name: str) -> None:
    """List all the pages defined in the flow

    Parameter
    ---------
    location: str
        location of the agent
    full_intent_name: str
        Format: projects/<Project ID>/locations/<Location ID>/agents/<Agent ID>/intents/<Intent ID>

    Returns
    -------
    None
    """

    client = dialogflowcx_v3.IntentsClient(client_options=ClientOptions(api_endpoint=f"{location}-dialogflow.googleapis.com"))

    # Initialize request argument(s)
    request = dialogflowcx_v3.DeleteIntentRequest(
        name=full_intent_name
    )

    # Make the request
    client.delete_intent(request=request)


def list_intents(location: str, agent_name: str) -> dialogflowcx_v3.ListIntentsResponse:
    """List all the flows defined in the agent

    Parameter
    ---------
    location: str
        location of the agent
    agent_name: str
        Format: projects/<project-id>/locations/<location>/agents/<agent-id>

    Returns
    -------
    List of all the intents
    """

    client = dialogflowcx_v3.IntentsClient(client_options=ClientOptions(api_endpoint=f"{location}-dialogflow.googleapis.com"))

    # Initialize request argument(s)
    request = dialogflowcx_v3.ListIntentsRequest(
        parent=agent_name
    )

    # Make the request
    response = client.list_intents(request=request)
    return response


def find_parent_intent_with_examples(flow_intents: set) -> set:
    """Finds set of parent intents with training phrases"""

    # parent intents with examples
    parent_intent_with_examples = set()
    for intent1 in flow_intents:
        for intent2 in flow_intents:
            if intent2.startswith(intent1) and intent1 != intent2:
                parent_intent_with_examples.add(intent1)
                break

    # print(*parent_intent_with_examples,sep="\n")

    return parent_intent_with_examples


def find_all_and_flow_intents(filedir: str) -> tuple:
    """Finds all intents and flow intents from the exported agent"""

    if "flows" in listdir(filedir):
        flow_paths = join(filedir, "flows")
    else:
        raise Exception("flows does not exist in the given dir")

    final_df = pandas.DataFrame()
    for root, dirs, files in os.walk(join(filedir, "flows"), topdown=False):
        for file in files:
            page_file = join(root, file)
            flow_name = find_flow_name_from_path(page_file)
            with open(page_file, mode="r", encoding="utf8") as f:
                page = json.load(f)
                if "transitionRoutes" in page:
                    df = pandas.json_normalize(page["transitionRoutes"], sep="-")
                    df["page"] = re.sub(".json$", "", file)
                    df["flow_name"] = flow_name
                    final_df = pandas.concat([final_df, df], ignore_index=True)
                else:
                    # print(f"tr is not present in {page_file}")
                    pass
    final_df["tags"] = final_df[["flow_name", "page"]].apply(set_tag, axis=1)
    final_df["page"] = final_df[["flow_name", "page"]].apply(set_start_page, axis=1)

    # to avoid cases with transition route defined but does not detect any intents (for reading params)
    final_df = final_df[["intent", "page", "tags", "flow_name"]][pandas.notna(final_df["intent"])]

    print(final_df)
    intent_path = join(filedir, "intents")
    all_intents = set()
    if exists(intent_path):
        for intent in listdir(intent_path):
            intent_folder = join(intent_path, intent)
            for json_file in listdir(intent_folder):
                if isfile(join(intent_folder, json_file)):
                    all_intents.add(urllib.parse.unquote(re.sub(".json", "", json_file)))

    flow_intents = set(final_df.intent.values.tolist())

    return all_intents, flow_intents


def set_start_page(row: pandas.Series) -> str:
    """Sets the start page"""

    # if page name and the flow name are same, then it is a start page
    if row.page == row.flow_name:
        return "start"
    return row.page


def set_tag(row: pandas.Series) -> str:
    """Sets the tag"""

    if row.flow_name == row.page:
        return row.flow_name
    return f'{row.flow_name}:{row.page}'


def find_flow_name_from_path(path: str) -> str:
    """Finds flow name from the path of the file"""

    list_path = path.split("/")
    if "pages" in list_path:
        return list_path[-3]
    return list_path[-2]
