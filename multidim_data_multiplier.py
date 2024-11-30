"""
python multidim_data_multiplier.py

Makes more data from existing data.

Looks at all the existing data in the file
Initial load is just 60 files through may nad june 2022 but that may have grown
Works out the next day
Checks if there a matching day using may for odd months, and june for even months.
If there isn't goes onto the next day till it finds a day with base daata
Takes all the conversations from that day file
For each conversation appends an additional three words to the end of every utterance.
These are generated on basis of

year 100 words 
month 12 words
day 31 words

year + month + day = 2024 12 30 = 2074 
abcd_id 100042 words
example_id





(so original and modified conversations can be identified)



"""
# ******************************************************************************************************************120

# standard imports
import re
import os
import dateutil
import datetime
import json
import random

# 3rd party imports
import click
import pandas

# custom imports
import humanfirst
import multidim_loader
import multidim_data_generation

@click.command()
@click.option('-f', '--input_folder', type=str, required=True, help='Name of input files')
@click.option('-m', '--max_files', type=int, required=True, help='Limit number of files to this') 
@click.option('-p', '--prefix', type=str, required=False, default="abcd", help='Prefix for input')
def main(input_folder: str,
         max_files: int,
         prefix: str) -> None: # pylint: disable=unused-argument
    """Main Function"""

    hf_gen = humanfirst.generators.HFGEN()

    for j in range(max_files):
        multidim_data_generation.logit("file",j)
        
        # read input directory
        load_files = multidim_loader.read_input_directory(input_folder=input_folder,prefix=prefix)
        last_file = load_files[-1]
        assert isinstance(last_file,str)

        # extract latest date from the last and get the next date to work on
        re_latest_date = re.compile(prefix + "-([0-9]{,4}-[0-9]{,2}-[0-9]{,2})")
        matches = re_latest_date.search(last_file)
        last_file_date = matches.group(1)
        multidim_data_generation.logit("last_file_date",last_file_date)
        next_file_date = str(dateutil.parser.parse(last_file_date) + datetime.timedelta(days=1))[0:10]
        multidim_data_generation.logit("next_file_date",next_file_date)
        
        source_date_found = False
        i = 0
        while not source_date_found and i < 30:
            # work out if odd or even
            if int(next_file_date[5:7]) % 2 == 0:
                # even
                parent_month = "06"
            else:
                # odd
                parent_month = "05"
            
            
            # Work out source file date - this might return dates that don't exist - but then they wont be found which is fine
            source_file_date = "2022-" + parent_month + next_file_date[7:10]
            
            
            source_file = f'{prefix}-{source_file_date}.json'
            source_file = os.path.join(input_folder,source_file)
            
            if source_file in load_files:
                source_date_found = True       
                multidim_data_generation.logit("source_file found",source_file)
            else:
                multidim_data_generation.logit("source_file not found",source_file)
                next_file_date = str(dateutil.parser.parse(next_file_date) + datetime.timedelta(days=1))[0:10]
                multidim_data_generation.logit("next_file_date",next_file_date)
                i = i + 1
            
        
        # read the source file
        file_in = open(source_file,mode="r",encoding="utf8")
        convos_dict = json.load(file_in)
        file_in.close()
        df_convos = pandas.json_normalize(convos_dict["examples"])
        
        # update the text 
        large_word_list = hf_gen.get_large_word_list()
        df_convos = df_convos.apply(add_words_to_text,axis=1,args=[next_file_date,large_word_list])
        
        print(df_convos)
        
        # turn back to a conversation
                
        # turn it back into hf json
        json_output = multidim_data_generation.denormalize_to_hf_json(df_convos,delimiter=".")
        filename = f'{prefix}-{next_file_date}.json'
        filename = os.path.join(input_folder,filename)
        
        with open(filename,mode="w",encoding="utf8") as file_out:
            json.dump(json_output,file_out,indent=2)
            multidim_data_generation.logit("wrote to",filename)
        
   
def add_words_to_text(row: pandas.Series, next_file_date: str, large_word_list: list) -> pandas.Series:
    """update the text based on ABCD and date and set conversation_date for download"""
    
    # ABCD uniqueness
    first_word_index = int(row["context.context_id"]) # abcd id
    first_word = large_word_list[first_word_index]
    
    # Date uniqueness - all convos should be within the same date as auto generated
    second_word_index = int(next_file_date[0:4]) + int(next_file_date[5:7]) + int(next_file_date[9:10]) # date
    second_word = large_word_list[second_word_index]
    
    # Turn uniqueness
    third_word_index = int(row["metadata.conversation_turn"])
    random.seed(third_word_index) # make it replicatable 
    third_word_index = random.randrange(0,len(large_word_list)) # just to make the selected words a bit more interesting   
    third_word = large_word_list[third_word_index]
    
    # Update the text
    row["text"] = str(row["text"]) + " " + first_word + " " + second_word + " " + third_word
    
    # set the conversation date so that the results can be downloaded
    row["metadata.date_of_convo"] = next_file_date
    
    # also need to set created_at
    row["created_at"] = next_file_date + row["created_at"][10:]
    
    # also regenerate the conversation id and example id
    
    # as they are just strings, just use my unique words
    row["id"] = row["id"] + "-" + first_word + "-" + second_word + "-" + third_word
    
    # convo id needs to be consistent across utterances
    row["context.context_id"] = str(row["context.context_id"]) + "-" + first_word + "-" + second_word
    
    
    return row
           
if __name__ == '__main__':
    main() # pylint: disable=no-value-for-parameter
