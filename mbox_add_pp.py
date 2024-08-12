"""
python mbox_add_pp.py

mbox additional post processing

Read all the json.
Create a mirror folder structure so we don't lose the original
Write to the new breaking out new fields
- list of every email involved anywhere in the message
- sorting out the formatting
- removing the underlying link (which often contain a lot of trackers and use a lot of tokens)

TODO: if this were a dataframe could multi thread.
But could also with months by slicing through a map of the sub docs?

"""
# ******************************************************************************************************************120

# standard imports
import os
import json
import typing
import re
import pathlib

# 3rd party imports
import click
import tiktoken


# Email address definitions
# https://www.regular-expressions.info/email.html
# The official standard is known as RFC 5322.
# It describes the syntax that valid email addresses must adhere to.
# You can (but you shouldn’t—read on) implement it with the following regular expression.
# RFC 5322 leaves the domain name part open to implementation-specific choices that won’t work on the Internet today.
# The regex implements the “preferred” syntax from RFC 1035 which is one of the recommendations in RFC 5322:
#
# # \A(?:[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*
#  |  "(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21\x23-\x5b\x5d-\x7f]
#       |  \\[\x01-\x09\x0b\x0c\x0e-\x7f])*")
# @ (?:(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?
#   |  \[(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}
#        (?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?|[a-z0-9-]*[a-z0-9]:
#           (?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21-\x5a\x53-\x7f]
#           |  \\[\x01-\x09\x0b\x0c\x0e-\x7f])+)
#      \])\z
#
# We get a more practical implementation of RFC 5322 if we omit IP addresses,
# domain-specific addresses, the syntax using double quotes and square brackets.
# It will still match 99.99% of all email addresses in actual use today.
# \A[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*
# @(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\z

#  CASE INSENSITIVE

# custom imports

# REGEX setup - emails
PT_A_EMAIL = r"[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*"
PT_B_EMAIL = r"@(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?"
re_extract_emails = re.compile(PT_A_EMAIL + PT_B_EMAIL,flags=re.IGNORECASE)

# REGEX long links
PT_A_FILETYPE = r"(http|ftp|https)" # GROUP1
PT_B_COLON_SLASHES = r":\/\/"
PT_C_FIRST_DOMAIN_PART = r"([\w_-]+(?:(?:\.[\w_-]+)+))" # GROUP2
PT_D_THE_REST = r"([\w.,@?^=%&:\/~+#-]*[\w@?^=%&\/~+#-])" #GROUP3
re_long_links = re.compile(
    PT_A_FILETYPE +
    PT_B_COLON_SLASHES +
    PT_C_FIRST_DOMAIN_PART +
    PT_D_THE_REST
)

# work out embeddings
embeddings = tiktoken.encoding_for_model("gpt-4o")

TRUNCATE_AT_TOKENS = 8191

@click.command()

@click.option('-d', '--directory', type=str, required=True, help='Directory')
@click.option('-o', '--output_directory', type=str, required=True, default=False,
              help='Output dir replacement')
@click.option('-r', '--reverse', type=bool, required=False, default=False,
              help='String sort ascheding or descending')
@click.option('-m', '--max_records', type=int, required=False, default=0,
              help='Stop after this many')
def main(directory: str,
         output_directory: str,
         reverse: bool,
         max_records: int) -> None: # pylint: disable=unused-argument
    """Main Function
    make sure output directory fully relative path """

    # regex replacement quite simple for target
    re_replace_original = re.compile(f'{directory}')
    print(f'Writing to output_directory: {work_out_target(re_replace_original,directory,output_directory)}')
    assert directory != output_directory

    process_dir(directory,reverse,re_replace_original,output_directory,do_all_these_things,max_records)

def work_out_target(re_replace_original: re, directory: str, output_directory: str):
    """Just replaces output_directory in directory using re without recompiling it"""
    return re_replace_original.sub(output_directory,directory)

def do_all_these_things(record: dict) -> dict:
    """ Orchestratator function"""

    # if the text is too_big it's just not worth dealing with - going to truncate it at tokens
    if record["tokens"] >= TRUNCATE_AT_TOKENS:
        record["content"] = embeddings.decode(embeddings.encode(record["content"])[0:TRUNCATE_AT_TOKENS-1])
        print(f'Trimmed: {record["filename"]}')

    # get the participants
    record["from_participants"] = extract_emails(record["From"])
    if len(record["from_participants"]) > 0:
        record["from_participant"] = record["from_participants"][0]
    record["to_participants"] = extract_emails(record["To"])
    record["to_participants_str"] = ','.join(record["to_participants"])
    record["cc_participants"] = extract_emails(record["Cc"])
    record["cc_participants_str"] = ','.join(extract_emails(record["Cc"]))
    record["participants"] = extract_emails(record["content"])
    record["participants_str"] = ','.join(record["participants"])

    # work on links
    record["shrunk_content"] = replace_long_links(record["content"])

    # recalculate tokens
    record["tokens_shrunk"] = len(embeddings.encode(record["shrunk_content"]))
    record["tokens_difference"] = record["tokens"] - record["tokens_shrunk"]

    return record

def extract_emails(content: str) -> list:
    """Return a deduplicated set of emails from anywhere in the text transformed record"""
    # has no groups so should return the list
    matches = re_extract_emails.findall(content)
    if isinstance(matches,list):
        return list(set(matches))
    else:
        return []

def replace_long_links(content: str) -> list:
    """Replace long links"""
    # has no groups so should return the list
    return re_long_links.sub("link",content)

def write_output(record: dict, re_replace_original: re, fqp: str, output_directory: str):
    """Writes the file"""
    output_fqp = work_out_target(re_replace_original,fqp,output_directory)
    with open(output_fqp,mode='w',encoding='utf8') as file_out:
        json.dump(record,file_out,indent=2)

def process_dir(directory:str, reverse: bool,
                re_replace_original: str, output_directory: str,
                call_this: typing.Callable, max_records: int):
    """Iterate through the folder structure creating a mirror one
    call_this must be a function accepting a dict representing a json, a directory and a fn"""


    # zero directory json ount
    dir_count = 0
    assert os.path.isdir(directory)

    # check output
    working_output_dir = work_out_target(re_replace_original, directory, output_directory)
    if not os.path.isdir(working_output_dir):
        thispath = pathlib.Path(working_output_dir)
        thispath.mkdir(parents=True)
        print(f'Created: {working_output_dir}')

    # create list
    list_files = os.listdir(directory)
    list_files.sort(reverse=reverse)

    # for every file here
    for fn in list_files:
        # work out fully qualified
        fqp = os.path.join(directory,fn)

        # if it is a file do the job passing the json
        if os.path.isfile(fqp):
            if fqp.endswith('.json'):
                with open(fqp,mode='r',encoding='utf8') as file_in:
                    json_dict = json.load(file_in)
                    json_dict['filename'] = fn
                    json_dict = call_this(json_dict)
                    write_output(json_dict,re_replace_original,fqp,output_directory)
                    dir_count = dir_count + 1
        elif os.path.isdir(fqp):
            sub_count = process_dir(fqp,reverse,re_replace_original,output_directory,call_this,max_records)
            dir_count=dir_count + sub_count
        else:
            print(fqp)
            raise RuntimeError("WTF?")
        if max_records > 0 and dir_count > max_records:
            break
    print(f'{dir_count:>10}    {directory}')
    return dir_count

if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
