#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ***************************************************************************80
#
# python coverage.py -f <your input file relative path>
#
# *****************************************************************************

# standard imports
import json

# third part imports
import pandas
import click
import copy

# custom imports
import humanfirst
import humanfirst_apis


@click.command()
@click.option('-u', '--username', type=str, default='', help='HumanFirst username if not providing bearer token')
@click.option('-p', '--password', type=str, default='', help='HumanFirst password if not providing bearer token')
@click.option('-n', '--namespace', type=str, required=True, help='HumanFirst namespace')
@click.option('-b', '--playbook', type=str, required=True, help='HumanFirst playbook id')
@click.option('-t', '--bearertoken', type=str, default='', help='Bearer token to authorise with')
@click.option('-c', '--convsetsource', default='', help='Filter results by a certain conversationset')
@click.option('-x', '--searchtext', default='', help='text to search for in conversations')
@click.option('-s', '--startisodate', default='', help='Date range to extract conversations from')
@click.option('-e', '--endisodate', default='', help='Date range to extract conversations from')
def main(username: str, password: int, namespace: bool, playbook: str, bearertoken: str, convsetsource: str, searchtext: str, startisodate: str, endisodate: str):
    write_coverage_csv(username, password, namespace, playbook, bearertoken,
            convsetsource, searchtext, startisodate, endisodate)


def write_coverage_csv(username: str, 
            password: int, 
            namespace: bool, 
            playbook: str, 
            bearertoken: str, 
            convsetsource: str, 
            searchtext: str, 
            startisodate: str, 
            endisodate: str,
            output_path: str = './data',
            separator: str = ',',
            page_size: int = 50):
    '''Download the full unlabelled model for the conversation set source with all the data science statistics
    inferred from the provided playbook then write a csv containing prediction data to the path provided with the
    separator provided'''

    headers = humanfirst_apis.process_auth(bearertoken, username, password)
    playbook_dict = humanfirst_apis.get_playbook(headers, namespace, playbook)
    df = get_conversationset_df(headers, namespace, playbook, convsetsource,
                                searchtext, startisodate, endisodate, playbook_dict, page_size=page_size)
    
    if not output_path.endswith('/'):
        output_path = output_path + '/'

    df.to_csv(f'{output_path}{playbook_dict["name"]}', sep=separator, encoding="utf8", index=False)
    print(df)

def get_conversationset_df(
        headers: dict,
        namespace: str,
        playbook: str,
        convsetsource: str,
        searchtext: str,
        startisodate: str,
        endisodate: str,
        playbook_dict: dict,
        page_size: int = 50) -> pandas.DataFrame:
    '''Download the inferred statistics for the conversation set source for the provided
    playbook and return a data frame.  Pages through the very large data science data
    with each page of page_size'''
    labelled_workspace = humanfirst.HFWorkspace.from_json(playbook_dict)
    assert (isinstance(labelled_workspace, humanfirst.HFWorkspace))
    intent_name_index = labelled_workspace.get_intent_index(delimiter="-")
    print("Got playbook and parsed it")

    i = 0
    results = []
    response_json = humanfirst_apis.query_conversation_set(
        headers,
        namespace,
        playbook,
        search_text=searchtext,
        start_isodate=startisodate,
        end_isodate=endisodate,
        convsetsource=convsetsource,
        page_size=page_size
    )
    results = extract_results(results, intent_name_index, response_json)
    assert (isinstance(response_json, dict))
    print(f'Page {i}: {len(results)}')
    i = i + 1

    while "nextPageToken" in response_json:
        response_json = humanfirst_apis.query_conversation_set(
            headers,
            namespace,
            playbook,
            search_text=searchtext,
            start_isodate=startisodate,
            end_isodate=endisodate,
            convsetsource=convsetsource,
            page_size=page_size,
            nextPageToken=response_json["nextPageToken"]
        )
        assert (isinstance(response_json, dict))
        if not "results" in response_json.keys() and "totalCount" in response_json.keys():
            print(f'totalCount: {response_json["totalCount"]}')
            break
        else:
            results = extract_results(
                results, intent_name_index, response_json)
        print(f'Page {i}: {len(results)}')
        i = i + 1

    return pandas.DataFrame(results)


def extract_results(results: list, intent_name_index: dict, response_json):
    '''Extacts the desired data for the data frame from a set of results from the query end point in HumanFirst'''
    for result in response_json["results"]:

        conv_obj = {}
        conv_obj["convoid"] = result["annotatedConversation"]["conversation"]["id"]
        # conv_obj["sources"] = result["annotatedConversation"]["conversation"]["sources"]
        conv_obj["conv_created_at"] = result["annotatedConversation"]["conversation"]["createdAt"]
        conv_obj["conv_updated_at"] = result["annotatedConversation"]["conversation"]["updatedAt"]

        for i in range(len(result["annotatedConversation"]["conversation"]["inputs"])):
            try:
                if "value" in result["annotatedConversation"]["conversation"]["inputs"][i].keys():
                    conv_obj["utterance"] = result["annotatedConversation"]["conversation"]["inputs"][i]["value"]
                else:
                    conv_obj["utterance"] = ""
                conv_obj["role"] = result["annotatedConversation"]["conversation"]["inputs"][i]["source"]
                if "matches" in result["annotatedConversation"]["annotations"]["inputs_intents"]["inputs"][i].keys():
                    conv_obj["intent_id"] = result["annotatedConversation"]["annotations"]["inputs_intents"]["inputs"][i]["matches"][0]["intentId"]
                    conv_obj["score"] = result["annotatedConversation"]["annotations"]["inputs_intents"]["inputs"][i]["matches"][0]["score"]
                    conv_obj["intent"] = intent_name_index[conv_obj["intent_id"]]
                else:
                    for f in ["intent_id", "score", "intent"]:
                        conv_obj[f] = None
                conv_obj["seq"] = i
                results.append(copy.deepcopy(conv_obj))
            except Exception as e:
                print("No idea what's up with this:")
                print(json.dumps(result["annotatedConversation"]
                      ["conversation"]["inputs"][i], indent=2))
    return results


if __name__ == '__main__':
    main()
