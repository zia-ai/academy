"""
python coverage.py

Set HF_USERNAME and HF_PASSWORD as environment variables
"""
# *****************************************************************************

# standard imports
import json
import copy

# third part imports
import pandas
import click
import humanfirst


@click.command()
@click.option('-u', '--username', type=str, default='',
              help='HumanFirst username if not setting HF_USERNAME environment variable')
@click.option('-p', '--password', type=str, default='',
              help='HumanFirst password if not setting HF_PASSWORD environment variable')
@click.option('-n', '--namespace', type=str, required=True, help='HumanFirst namespace')
@click.option('-b', '--playbook', type=str, required=True, help='HumanFirst playbook id')
@click.option('-c', '--convsetsource', type=str, default='', help='Filter results by a certain conversationset')
@click.option('-x', '--searchtext', type=str, default='', help='text to search for in conversations')
@click.option('-s', '--startisodate', type=str, default='', help='Date range to extract conversations from')
@click.option('-e', '--endisodate', type=str, default='', help='Date range to extract conversations from')
@click.option('-q', '--quit_after_pages', type=int, default=0, help='Date range to extract conversations from')
@click.option('-d', '--debug', is_flag=True, default=False, help='Debug')
@click.option('-l','--delimiter',type=str,default="-",help='Intent name delimiter')
def main(username: str, password: str, namespace: bool, playbook: str,
         convsetsource: str, searchtext: str, startisodate: str,
         endisodate: str, quit_after_pages: int, debug: bool, delimiter: str):
    '''Main function'''
    write_coverage_csv(username, password, namespace, playbook,
                       convsetsource, searchtext, startisodate, endisodate, delimiter=delimiter,
                       quit_after_pages=quit_after_pages, debug=debug)


def write_coverage_csv(username: str,
                       password: str,
                       namespace: bool,
                       playbook: str,
                       convsetsource: str,
                       searchtext: str,
                       startisodate: str,
                       endisodate: str,
                       delimiter: str,
                       output_path: str = './data',
                       separator: str = ',',
                       page_size: int = 50,
                       quit_after_pages: int = 0,
                       debug: bool = False):
    '''Download the full unlabelled model for the conversation set source with all the data science statistics
    inferred from the provided playbook then write a csv containing prediction data to the path provided with the
    separator provided'''

    hf_api = humanfirst.apis.HFAPI(username=username, password=password)
    playbook_dict = hf_api.get_playbook(namespace, playbook)

    df = get_conversationset_df(hf_api, namespace, playbook, convsetsource,
                                searchtext, startisodate, endisodate, playbook_dict, delimiter,
                                page_size=page_size, quit_after_pages=quit_after_pages, debug=debug)

    if not output_path.endswith('/'):
        output_path = output_path + '/'

    workspace_name = str(playbook_dict["name"]).replace(" ","_")
    workspace_name = workspace_name.replace("-","_")

    output_file_uri = f'{output_path}{workspace_name}.csv'

    df.to_csv(output_file_uri, sep=separator, encoding="utf8", index=False)
    print(df)
    print(f'Wrote to: {output_file_uri}')


def get_conversationset_df(
        hf_api : humanfirst.apis.HFAPI,
        namespace: str,
        playbook: str,
        convsetsource: str,
        searchtext: str,
        startisodate: str,
        endisodate: str,
        playbook_dict: dict,
        delimiter: str,
        page_size: int = 50,
        quit_after_pages: int = 0,
        debug: bool = False) -> pandas.DataFrame:
    '''Download the inferred statistics for the conversation set source for the provided
    playbook and return a data frame.  Pages through the very large data science data
    with each page of page_size'''
    labelled_workspace = humanfirst.objects.HFWorkspace.from_json(playbook_dict,delimiter=delimiter)
    assert isinstance(labelled_workspace, humanfirst.objects.HFWorkspace)
    intent_name_index = labelled_workspace.get_intent_index(delimiter="-")
    print("Got playbook and parsed it")

    i = 0
    results = []
    response_json = hf_api.query_conversation_set(
        namespace,
        playbook,
        search_text=searchtext,
        start_isodate=startisodate,
        end_isodate=endisodate,
        convsetsource=convsetsource,
        page_size=page_size
    )
    # with open("./data/testing_coverage_123.json",mode="w",encoding="utf8") as fileobj:
    #     json.dump(response_json,fileobj,indent=2)
    # quit()
    results = extract_results(
        results, intent_name_index, response_json, debug=debug)
    assert isinstance(response_json, dict)
    print(f'Page {i}: {len(results)}')
    i = i + 1

    print(f"Quit after pages {quit_after_pages}")
    while "nextPageToken" in response_json:
        if quit_after_pages > 0 and i >= quit_after_pages:
            break
        try:
            response_json = hf_api.query_conversation_set(
                namespace,
                playbook,
                search_text=searchtext,
                start_isodate=startisodate,
                end_isodate=endisodate,
                convsetsource=convsetsource,
                page_size=page_size,
                next_page_token=response_json["nextPageToken"]
            )
        except Exception as e: # pylint: disable=broad-exception-caught
            print(f"Error - {e}")
            print("Retrying")
            continue

        assert isinstance(response_json, dict)
        if not "results" in response_json.keys() and "totalCount" in response_json.keys():
            print(f'totalCount: {response_json["totalCount"]}')
            break
        else:
            results = extract_results(
                results, intent_name_index, response_json)
        print(f'Page {i}: {len(results)}')
        i = i + 1

    return pandas.DataFrame(results)


def dump_this(text: str, filename: str):
    """Dump text to file in debug mode"""
    with open(f"./data/{filename}.json", "w", encoding="utf8") as file_out:
        file_out.write(json.dumps(text, indent=2))


def extract_results(results: list, intent_name_index: dict, response_json, debug: bool = False):
    '''Extacts the desired data for the data frame from a set of results from the query end point in HumanFirst'''
    for result in response_json["results"]:

        conv_obj = {}
        conv_obj["convoid"] = result["annotatedConversation"]["conversation"]["id"]
        # conv_obj["sources"] = result["annotatedConversation"]["conversation"]["sources"]
        conv_obj["conv_created_at"] = result["annotatedConversation"]["conversation"]["createdAt"]
        conv_obj["conv_updated_at"] = result["annotatedConversation"]["conversation"]["updatedAt"]

        if debug:
            dump_this(result, conv_obj["convoid"])

        for i in range(len(result["annotatedConversation"]["conversation"]["inputs"])):
            try:
                if "value" in result["annotatedConversation"]["conversation"]["inputs"][i].keys():
                    conv_obj["utterance"] = result["annotatedConversation"]["conversation"]["inputs"][i]["value"]
                else:
                    conv_obj["utterance"] = ""
                conv_obj["role"] = result["annotatedConversation"]["conversation"]["inputs"][i]["source"]
                if "matches" in result["annotatedConversation"]["annotations"]["inputs_intents"]["inputs"][i].keys():
                    conv_obj["intent_id"] = result["annotatedConversation"]["annotations"]["inputs_intents"]["inputs"][i]["matches"][0]["intentId"] # pylint: disable=line-too-long
                    conv_obj["score"] = result["annotatedConversation"]["annotations"]["inputs_intents"]["inputs"][i]["matches"][0]["score"] # pylint: disable=line-too-long
                    conv_obj["intent"] = intent_name_index[conv_obj["intent_id"]]
                else:
                    for field in ["intent_id", "score", "intent"]:
                        conv_obj[field] = None
                conv_obj["seq"] = i
                results.append(copy.deepcopy(conv_obj))
            except Exception: # pylint: disable=broad-exception-caught
                print("No idea what's up with this:")
                print(json.dumps(result["annotatedConversation"]
                      ["conversation"]["inputs"][i], indent=2))
    return results


if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
