#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ***************************************************************************80**************************************120
#
# humanfirst.py
#
# a set of helper classes and methods for describing, validating and interacting with HFOBjects
# that make up the HF JSON format
#
# https://numpydoc.readthedocs.io/en/latest/format.html
#
#
# ***************************************************************************80**************************************120

import datetime
import hashlib
import json
import random
from typing import IO, Any, Dict, List, Optional, Union
from dataclasses import dataclass, field
from dataclasses_json import dataclass_json
import pandas

HFMetadata = Dict[str, Any]


@dataclass_json
@dataclass
class HFTag:
    '''Schema object for HF Tags

    Validates the format of a tag

    Tags can be added HFIntents (intent level) or HFExamples (utterance level)
    In general separate tags should be used for each

    Parameters
    ----------
    id:    str            unique id for tag
    name:  str            name of tag that will be displayed in HF studio
    color: str, optional  a hex code starting with # for a color to display the tag in eg #ff33da (a bright pink)
                          if a color isn't provided a random one will be assigned
    '''
    id: str
    name: str
    color: Optional[str] = None

    def __init__(self, id: str, name: str, color: Optional[str] = None):
        self.id = id
        self.name = name
        if color and color != '':
            self.color = color
        else:
            self.color = '#' + ''.join([random.choice('0123456789ABCDEF') for j in range(6)])


@dataclass_json
@dataclass
class HFIntent:
    '''Schema object for HF Intent

     Validates the format of an Intent

     Parameters
     ----------
     id:        str            unique id for intent
     name:      str            name of intent that will be displayed in HF studio
     metadata:  dict           a dictionary or HFMetadata object of string only key value pairs
     tags:      list           a list of HFTag objects
     parent_intent_id: str, optional  a reference to the ID of the immediate parent if using hierarchy intents
     '''
    id: str
    name: str
    metadata: HFMetadata = field(default_factory=dict)
    tags: List[HFTag] = field(default_factory=list)
    parent_intent_id: Optional[str] = None

    def __init__(self, id: str, name: str, metadata: HFMetadata = {}, tags: List[HFTag] = [], parent_intent_id: Optional[str] = None):
        self.id = id
        self.name = name
        self.parent_intent_id = parent_intent_id
        self.metadata = metadata
        self.tags = tags


@dataclass_json
@dataclass
class HFContext:
    '''Schema object for HFContext

    Validates the format of a the Context object interelating multiple HFExamples (utterances)
    within a document or conversation.  Th

    Parameters
    ----------
    context_id: str             unique id for context object
    type      : str, optional   the document type, only "conversation" is currently supported
                                will control how the utterances display in multi utterance GUI items
    role      : str, optional   two roles are defined for conversation document 
                                'client' - the analysed party in the tool typically the human user, customer etc.
                                'expert' - the responding party in the tool typically the bot, agent etc.    
    '''
    context_id: Optional[str] = None
    type: Optional[str] = None
    role: Optional[str] = None

    def __init__(self, context_id: Optional[str] = None, type: Optional[str] = None, role: Optional[str] = None):
        self.context_id = context_id
        if type and type != '':
            if type in ['conversation']:
                self.type = type
            else:
                raise Exception ('Only "conversation" document type is currently supported')
        if role and role != '':
            if role in ['expert','client']:
                self.role = role
            else:
                raise Exception ('Only "conversation" document with roles of "client" or "expert" are currently supported')
    

@dataclass_json
@dataclass
class HFIntentRef:
    '''Schema object for HFIntentRef - a reference to another intent typically for identifying the parent in a hierarchy

    Validates the format of a the HFIntentRef Object

    Parameters
    ----------
    intent_id: str  the id of the referenced intent
    '''
    intent_id: str

    def __init__(self, intent_id: str):
        self.intent_id = intent_id


@dataclass_json
@dataclass
class HFExample:
    '''Schema object for HFExample - a labelled or unlabelled utterance example

    Text from a document of some kind.
    May be a single utterance or be linked by a HFContext object to other examples forming a document
    May contain metadata about where was created useful to an annotator in the HF Studio

    TODO: annotation of entities

    Parameters
    ----------
    id:       str  An id for the Example
    text:     str  The text of the example
    context:  HFContext, optional  A HFContext object defining what document type the example came from
                                   defining what role the speaker/writer was performing and linking the 
                                   example to other examples making up that document
    intents:  list HFIntentRefs    A list of ids of intents for which this example text is an example of
                                   May be empty list [] if so the utterance will be treated as unlabelled
    tags:     list HFTags          A list of ids of intents for which this example text is an example of
                                   May be empty list [] if so the utterance will be treated as unlabelled
                                   and appear int the data section
                                   If provided these utterance will be treated as labelled and appear in the
                                   intents section
    metadata: dict | HFMetadata    A dict of string only key value pairs detailing information about the text
                                   useful to a future annotator                                  
    '''
    id: str
    text: str
    created_at: str
    intents: List[HFIntentRef] = field(default_factory=list)
    tags: List[HFTag] = field(default_factory=list)
    metadata: HFMetadata = field(default_factory=dict)
    context: Optional[HFContext] = None

    def __init__(self, text: str, id: str, created_at: Optional[datetime.datetime] = None, intents: List[HFIntent] = [], tags: List[HFTag] = [], metadata: HFMetadata = {}, context: Optional[HFContext] = None):
        self.id = id
        self.text = text
        self.intents = intents
        self.tags = tags
        self.metadata = metadata

        self.tags = tags
        self.metadata = metadata
        self.context = context

        if created_at is not None:
            if isinstance(created_at, str):
                self.created_at = created_at
            else:
                self.created_at = created_at.isoformat() + 'Z'

        if len(intents) > 0:
            self.intents = [HFIntentRef(intent.intent_id)
                            for intent in intents]


class HFWorkspace:
    '''Schema object for HFWorkspace - may be used to update labelled or unlabelled data to HF Studio

    Validates the overall workspace and all sub objects

    TODO: entities

    Attributes
    ----------
    TODO:
    
    '''
    intents: Dict[str, HFIntent]
    intents_by_id: Dict[str, HFIntent]
    examples: Dict[str, HFExample]
    tags: Dict[str, HFTag]
    

    def __init__(self):
        self.intents = {}
        self.intents_by_id = {}
        self.tags = {}
        self.examples = {}

    def intent(self, name_or_hier: Union[str, List[str]], id: Optional[str] = None, tags: List[HFTag] = [], metadata: HFMetadata = {}) -> HFIntent:
        '''Check whether the intent exists within the hierarchy provided, if it does return the intent object found
        If it does not, create it, along with all necessary parents that don't exist and return the new object

        Parameters
        ----------
        name_or_hier: str | List[str]    The name of the intent if no hierachy or the top level of an intent hierarchy
                                         i.e "billing"
                                         Or a list of names of intents in a list in the order of their hierarchy
                                         ["billing","issues","cannot_pay"]
        id:           str,optional       If not present will be generated as a repeatable hash of the text
        tags:         List[HFTags]       A list of tags placed on the intent and display in the tool
        metadata:     dict | HFMetadata  A dict of string only key value pairs detailing information about the text
                                         useful to an annotator in HF Studio                                  
        '''
        if type(name_or_hier) is not list:
            print("not list")
            name_or_hier = [name_or_hier]

        parent_intent_id = None
        last = None
        for part in name_or_hier:
            if part == '':
                break
            if part not in self.intents:
                if not id:
                    genid = 'intent-%d' % len(self.intents)
                else:
                    genid = id

                # TODO: this doesn't work if you want the parent intent to have different metadata or tags to the child intent
                # the first child intent creates the full hierarchy
                intent = HFIntent(
                    id=genid,
                    name=part,
                    parent_intent_id=parent_intent_id,
                    metadata=metadata,
                    tags=tags,
                )
                self.intents[part] = intent
                self.intents_by_id[genid] = intent
            last = self.intents[part]
            parent_intent_id = last.id

        return last
    
    def tag_intent(self,intent_id,tag: HFTag):
        # get the intent here
        intent = self.intent_by_id(intent_id)
        assert(isinstance(intent,HFIntent))
        for i in range(len(intent.tags)):
            assert(isinstance(intent.tags[i],HFTag))
            if intent.tags[i] == tag.name:
                intent.tags[i] = tag
                self.intents_by_id[intent_id] = intent
                print("tag_exists")
                return tag
        intent.tags.append(tag)
        self.intents_by_id[intent_id] = intent
        return tag
    
    def get_intent_index(self, delimiter: str) -> dict:
        # for every intent
        # go back up it's parent hierachy by id
        # reassemble name_or_hier
        # concatentate
        # in other file need to split and trim
        # hopefully should compare.
        intent_name_index = {}      
        for intent_id in self.intents_by_id:
            working = self.intents_by_id[intent_id]
            fullpath=working.name
            while working.parent_intent_id:
                working = self.intents_by_id[working.parent_intent_id]
                fullpath = f'{working.name}{delimiter}{fullpath}'
            intent_name_index[intent_id] = fullpath               
        return intent_name_index
    
    def write_csv(self,output_path: str) -> None:
        """Writes the HF Workspace to a CSV file"""

        intent_name_index = self.get_intent_index(delimiter="-")
        csv_obj = {}

        # creating a overall structure for the dataframe
        for intent_id in intent_name_index.keys():
            obj = {
                "intent_id": intent_id,
                "intent_name": intent_name_index[intent_id],
                "intent_metadata": self.intents_by_id[intent_id].metadata,
                "text": [],
                "text_metadata": []
            }
            csv_obj[intent_id] = obj

        # appending all the examples and their corresponding metadata to their respective intents
        for phrase_id in self.examples:
            example = self.examples[phrase_id]
            example_intent = example.intents[0].intent_id
            csv_obj[example_intent]["text"].append(example.text)
            csv_obj[example_intent]["text_metadata"].append(example.metadata)

        # create a dataframe
        list_obj = []
        for intent_id in csv_obj:
            list_obj.append(csv_obj[intent_id])
        df = pandas.json_normalize(list_obj,sep="-")
        df = df.explode(["text","text_metadata"],ignore_index=True)

        # create seperate columns for each metadata field in text_metadata column
        metadata_keys = self.find_all_utterance_metadata(df["text_metadata"])
        for i in range(len(metadata_keys)):
            metadata_keys[i] = f"text_metadata-{metadata_keys[i]}"

        df_empty = pandas.DataFrame(columns=metadata_keys)
        df = pandas.concat([df,df_empty],axis=0)
        df = df.apply(self.write_utterance_metadata,axis=1)
        df = df.drop(columns=["text_metadata"])
        print(df)
        
        df.to_csv(output_path,sep=",",encoding="utf8",index=False)

    def write_utterance_metadata(self,row: pandas.Series) -> pandas.Series:
        """Copies the text metadata values to the respective column"""

        if not isinstance(row.text_metadata,float):
            for key in row.text_metadata.keys():
                row[f"text_metadata-{key}"] = row.text_metadata[key]
        return row

    def find_all_utterance_metadata(self,text_metadata: pandas.Series) -> list:
        """Finds set of all the metadata keys used by all the examples"""

        text_metadata = text_metadata.to_list()
        keys = set()
        for metadata in text_metadata:
            if not isinstance(metadata,float):
                keys.update(metadata.keys())
        
        return list(keys)

    def intent_by_id(self, id: str) -> Optional[HFIntent]:
        '''Return a particular intent by id
        
        Parameters
        ----------
        id: str   id to return
        '''
        return self.intents_by_id.get(id)

    def tag(self, tag: str, color: Optional[str] = None):
        '''Check whether tag (i.e tag name) already exists, if it does return the tag object with that name
        If not create the tag object
        '''
        if tag not in self.tags:
            self.tags[tag] = HFTag('tag-%d' % len(self.tags), tag, color)
        return self.tags[tag]

    def example(self, text: str, id: Optional[str] = None, created_at: Optional[datetime.datetime] = None, intents: List[HFIntent] = [], tags: List[HFTag] = [], metadata: HFMetadata = {}, context: Optional[HFContext] = None) -> HFExample:
        '''Create a new example based on passed properties, assigning an ID if necessary
        '''
        if id is None:
            id = 'ex-%s' % hash_string(text)

        if id in self.examples:
            return self.examples[id]

        if created_at is None:
            created_at = datetime.datetime.now()

        ex = HFExample(
            text=text,
            id=id,
            created_at=created_at,
            intents=intents,
            tags=tags,
            metadata=metadata,
            context=context,
        )

        self.examples[ex.id] = ex

        return ex

    def add_example(self, example: HFExample):
        '''Add an example to the workspace based on an example created elsewhere using the HFExample constructor
        '''
        assert (isinstance(example, HFExample))          
        if example.id is None:
            raise Exception(
                'All examples must have an id to be included in a workspace?')
        self.examples[example.id] = example

    @staticmethod
    def from_json(input: Union[IO,dict]) -> 'HFWorkspace':
        '''Read and validate a HFWorkspace object from a dict of a json (from api)
        or from a json file
        '''
        if isinstance(input,IO):
            obj = HFWorkspaceJson.from_json(input.read(), infer_missing=True)
        elif isinstance(input,dict):
            obj = HFWorkspaceJson.from_json(json.dumps(input), infer_missing=True)
        else:
            raise Exception(f"What is this thing of type: {type(input)}")

        workspace = HFWorkspace()
        workspace.intents = {intent.name: intent for intent in obj.intents}
        workspace.intents_by_id = {intent.id: intent for intent in obj.intents}
        workspace.tags = {tag.id: tag for tag in obj.tags}
        workspace.examples = {example.id: example for example in obj.examples}

        return workspace

    def write_json(self, output: IO, jsonl=False, indent=2):
        '''Write workspace object into HF format for uploading to studio
        '''

        sorted_examples = list(self.examples.values())
        sorted_examples.sort(key=lambda ex: ex.created_at)
        workspace = {
            "examples": [ex.to_dict() for ex in sorted_examples],
        }

        if len(self.tags) > 0:
            workspace['tags'] = [tag.to_dict() for tag in self.tags.values()]

        if len(self.intents) > 0:
            workspace['intents'] = [intent.to_dict()
                                    for intent in self.intents.values()]

        if jsonl:
            indent = None

        json.dump(workspace, output, indent=indent)

        if jsonl:
            output.write('\n')


@dataclass_json
@dataclass
class HFWorkspaceJson:
    '''JSON version of the workspace'''
    examples: List[HFExample] = field(default_factory=list)
    intents: List[HFIntent] = field(default_factory=list)
    tags: List[HFTag] = field(default_factory=list)


def hash_string(s: str, prefix: Optional[str] = None) -> str:
    '''Hash a string into a repeatable id with an optional prefix
    lets you build    myprefix-guid from "Blah whatever"
    '''
    hexdigest = hashlib.new('sha256', s.encode('utf-8')).hexdigest()
    if prefix:
        return f'{prefix}-{hexdigest[0:20]}'
    else:
        return f'{hexdigest[0:20]}'