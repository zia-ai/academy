"""
MAKE SURE NLU IS TRAINED IN TARGET WORKSPACE

export HF_PASSWORD=<password>

python predict_utterance.py 
-i "Yo! It's going to need to be a new shipping address as my circumstances have changed"
-u <username>
-p $HF_PASSWORD
-n <namepspace>
-b <playbook-id>

To use with bearer token on on-prem
HF_BEARER=`hf auth print-access-token` 
or
HF_BEARER=`zia auth print-access-token` 

python predict_utterance.py --bearer $HF_BEARER -i "Utterance"

"""
# *****************************************************************************

# standard imports
import json

# third party imports
import click
import humanfirst


@click.command()
@click.option('-i','--input_uttr',type=str,required=True,help='Input utterance')
@click.option('-u', '--username', type=str, default='', help='HumanFirst username if not providing bearer token')
@click.option('-p', '--password', type=str, default='', help='HumanFirst password if not providing bearer token')
@click.option('-n', '--namespace', type=str, required=True, help='HumanFirst namespace')
@click.option('-b', '--playbook', type=str, required=True, help='HumanFirst playbook id')
@click.option('-t', '--bearertoken', type=str, default='', help='Bearer token to authorise with')
@click.option('-v', '--verbose',is_flag=True,default=False,help='Increase logging level')
@click.option('-m', '--maxresults',type=int,default=3,help='Maximum number of classes per utterance to display')
@click.option('-d', '--model_id',type=str,default=None,help='modelId of a specific model to query')
@click.option('-r', '--revision_id',type=str,default=None,help='revisionId of a specific model to query')
def main(input_uttr: str,
         username: str,
         password: int,
         namespace: bool,
         playbook: str,
         bearertoken: str,
         verbose: bool,
         maxresults: int,
         model_id: str,
         revision_id: str):
    """Main Function"""

    # do authorisation
    headers = humanfirst.apis.process_auth(bearertoken=bearertoken,username=username,password=password)

    # get the prediction
    response_dict = humanfirst.apis.predict(headers, input_uttr, namespace, playbook, model_id, revision_id)

    print("")
    print(f'Predict end point matches returned: {len(response_dict["matches"])}')
    # print revision id
    print('Prediction version info')
    print(f'modelId:    {response_dict["modelId"]}')
    print(f'revisionId: {response_dict["revisionId"]}')

    # print(json.dumps(humanfirst.apis.get_revisions(headers,namespace,playbook),indent=2))
    # print(json.dumps(humanfirst.apis.get_nlu_engine(headers,
    #                                                 namespace,
    #                                                 playbook,
    #                                                 nlu_id="nlu-RGS26OMK55H3ZKGSAMSBAKC6"),
    #                                                 indent=2))

    # cycle through the intents returned and also retreive metadata and display
    i =0
    for intent in response_dict['matches']:
        intent_full = humanfirst.apis.get_intent(headers, namespace, playbook, intent['id'])
        if i >= maxresults:
            break
        metadata = {}
        try:
            metadata = intent_full['metadata']
        except KeyError:
            pass
        # example of joining names to get fully qualified name
        print(f'{intent["score"]:.2f} {"-".join(intent["hierarchyNames"]):30} {metadata}')
        i = i+1

    # any entities entities
    print("")
    if 'entityMatches' in response_dict.keys():
        print("Entity matches:")
        for entity in response_dict['entityMatches']:
            e_key = entity["entity"]["key"]
            e_value = entity["entity"]["value"]
            e_span_start = entity["span"]["start"]
            e_span_end = entity["span"]["end"]
            print(
                f'@{e_key}:{e_value} start: {e_span_start} end: {e_span_end}')
    else:
        print("No entities detected:")

    # if verbose dump the whole predict response
    if verbose:
        print(json.dumps(response_dict,indent=2))
        print(json.dumps(intent_full,indent=2))

if __name__ == '__main__':
    main() # pylint: disable=no-value-for-parameter
