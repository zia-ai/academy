#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ***************************************************************************80
# before running the script set env varibale
#            - GOOGLE_APPLICATION_CREDENTIALS=<path to the gcp key>
# 
# python cleanse_intents.py -l location -a agent_name -d delimeter
# 
# description: delete intents that are not used in the flows 
#              and rename parent intents with training phrases
#
# *****************************************************************************

# standard imports
import click
from time import sleep

# custom imports
import dialogflow_cx_helper

@click.command()
@click.option('-l','--location',type=str,required=True,help='dialogflow cx agent location')
@click.option('-a','--agent_name',type=str,required=True,help='agent name')
@click.option('-d','--delimeter',type=str,required=True,help='intent delimiter')
def main(location: str, agent_name: str, delimeter: str) -> None:
    """Main Function
    
    Parameters
    ----------
    location: str
        location of the agent
    agent_name: str
        Format: projects/<project-id>/locations/<location>/agents/<agent-id>
    delimeter: str
        delimeter used to represent intent heirarchy
    
    Returns
    -------
    None

    """

    intent_list_response = dialogflow_cx_helper.list_intents(location,agent_name)
  
    intent_name_displayname_pair = {}
    intent_displayname_name_pair = {}
    intent_displayname_intentobj_pair = {}
    all_intents = set()
    for intent in intent_list_response:
        intent_displayname_name_pair[intent.display_name] = intent.name
        intent_name_displayname_pair[intent.name] = intent.display_name
        intent_displayname_intentobj_pair[intent.display_name] = intent
        all_intents.add(intent.display_name)
    
    flow_intents = find_flow_intents(location, agent_name, intent_name_displayname_pair)
    only_a = all_intents.difference(flow_intents)
    a_and_f = all_intents.intersection(flow_intents)
    print(f"Total number of intents(all_intents): {len(all_intents)}")
    print(f"Number of intents used in the flows(flow_intents): {len(flow_intents)}")
    print(f"Number of intents that are present in both all_intents and flow_intents: {len(a_and_f)}")
    print(f"Number of intents that are present only in all_intents and not in flow_intents(number of unused intents): {len(only_a)}")
    
    deleted_intents = []

    # delete intents    
    for intent in only_a:
        if intent != "Default Welcome Intent" and intent != "Default Negative Intent":
            sleep(1)
            dialogflow_cx_helper.delete_intent(location,intent_displayname_name_pair[intent])
            deleted_intents.append(intent)

    print(f"Total number of intents deleted: {len(deleted_intents)}")
    print(f"Total number of intents remaining in the CX agent: {len(all_intents) - len(deleted_intents)}")
    print(f"Intents that are skipped from deletion: {only_a-set(deleted_intents)}\n")

    # print(*deleted_intents,sep="\n")

    # intents rename
    parent_intent_with_examples = dialogflow_cx_helper.find_parent_intent_with_examples(flow_intents)
    print(f"Number of parent intent with examples: {len(parent_intent_with_examples)}")
    updated_intent_dict = {}
    for intent_displayname in parent_intent_with_examples:
        
        intent = intent_displayname_intentobj_pair[intent_displayname]
        updated_displayname = f"{intent.display_name}{delimeter}general"
        intent.display_name = updated_displayname

        # to avoid rate limit issues
        sleep(1)
        updated_intent = dialogflow_cx_helper.rename_intents(location,intent_displayname_intentobj_pair[intent_displayname])
        
        # update the intent obj in the dictionary
        del intent_displayname_intentobj_pair[intent_displayname]
        intent_displayname_intentobj_pair[updated_displayname] = updated_intent

        # changes made
        # print(f"{intent_displayname} -> {updated_displayname}")
        updated_intent_dict[intent_displayname] = updated_displayname
    
    print(f"\nNumber of renamed intents: {len(updated_intent_dict)}")

def find_flow_intents(location: str, agent_name: str, intent_name_displayname_pair: dict):
    """finds all the flow intents"""

    flows_list_response = dialogflow_cx_helper.list_flows(location,agent_name)
    flow_intents = set()
    for flow in  flows_list_response:
        # trg - transition route group
        trg_used_in_flow = set()

        # list of trgs that are used in flow start page
        trg_used_in_flow.update(set(flow.transition_route_groups))
        
        # list of trgs linked to flows
        trg_list_linked_to_flows = dialogflow_cx_helper.list_transition_route_groups(location,flow.name)
        
        # transition routes in all pages in a flow
        pages_response = dialogflow_cx_helper.list_pages(location,flow.name)
        for page in pages_response:
            for transition_route in page.transition_routes:
                if transition_route.intent in intent_name_displayname_pair.keys():
                    flow_intents.add(intent_name_displayname_pair[transition_route.intent])

            # list of trgs that are used in each page
            trg_used_in_flow.update(page.transition_route_groups)

        # parse through all the trgs linked to flow
        # for every trg, check if it is used in any pages in the flow
        # then add all the intents in the transition routes to the flow intents
        for page in trg_list_linked_to_flows.pages:
            for trg in page.transition_route_groups:
                if trg.name in trg_used_in_flow:
                    for tr in trg.transition_routes:
                        if tr.intent in intent_name_displayname_pair.keys():
                            flow_intents.add(intent_name_displayname_pair[tr.intent])
    
        # transition_routes in start page
        for transition_route in flow.transition_routes:
            if transition_route.intent in intent_name_displayname_pair.keys():
                flow_intents.add(intent_name_displayname_pair[transition_route.intent])
    return flow_intents

if __name__=="__main__":
    main()