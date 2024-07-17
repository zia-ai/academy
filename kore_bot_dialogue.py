"""
python kore_bot_dialogue.py -f <filename>

Extracts to csv
 - the intent model sentences
 - the trait model training data

Sentences organised by intent name
Traits organised by traitgroup and trait name

Designed to work on a complete kore.ai bot export file "botDialogue.json"

"""
# ******************************************************************************************************************120

# standard imports
import json

# 3rd party imports
import click
import pandas

# custom imports

@click.command()
@click.option('-f', '--filename', type=str, required=True, help='Input File Path')
def main(filename: str) -> None: # pylint: disable=unused-argument
    """Main Function"""
    
    # load json
    file_in = open(filename,mode="r",encoding="utf8")
    dict_in = json.load(file_in)
    file_in.close()
    
    # sentenaces
    df = pandas.json_normalize(dict_in["sentences"])
    print(df)  
    output_filename = filename.replace(".json","_training_output.csv")
    df.to_csv(output_filename,index=False)
    print(f'Wrote sentences to: {output_filename}')
    
    # traits
    tgs=[]
    for tg in dict_in["traits"]:
        traits = tg["traits"]
        assert isinstance(traits,dict)
        for t in traits.keys():
            for d in traits[t]["data"]:
                output_obj = {}
                output_obj
                for k in ['state', 'matchStrategy', 'scoreThreshold', 'groupName','language']:
                    output_obj[k] = tg[k]
                output_obj["trait_name"] = t
                output_obj["trait_display_name"] = traits[t]["displayName"]
                output_obj["data"] = d
                tgs.append(output_obj.copy())
    df_traits = pandas.json_normalize(tgs)
    print(df_traits)
    output_filename = filename.replace(".json","_traits_output.csv")
    df_traits.to_csv(output_filename,index=False)
    print(f'Wrote traits to: {output_filename}')    

if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
