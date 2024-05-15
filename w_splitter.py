"""
python w_splitter.py

"""
# ******************************************************************************************************************120

# standard imports
import re
import datetime
import html

# 3rd party imports
import click
import pandas

# custom imports
import humanfirst

@click.command()
@click.option('-f', '--filename', type=str, required=True, help='Input File Path')
def main(filename: str) -> None:
    """Main Function"""

    # read file
    df = pandas.read_csv(filename,delimiter=",",encoding="utf8")

    # regexes
    re_turn_splitter = re.compile("(User_[0-9]+:[ ]*|Assistant_[0-9]+:[ ]*)")
    re_splitter_splitter = re.compile("(User|Assistant)_([0-9])+:[ ]*")

    # variales for things looking for
    id = None
    w_role = None
    w_turn = None
    text = None

    # lists to keep them in
    w_roles = []
    w_turns = []
    w_text = []
    ids = []

    # cycle through convos
    for convo in df["conversations"]:

        # generate a unique Id for the conversation
        id = humanfirst.objects.hash_string(convo,prefix="convo")

        # split the conversation - retaining the splitter part
        utterance_candidates = re_turn_splitter.split(convo)

        # cycle through the splits
        for candi in utterance_candidates:
            matches = re_splitter_splitter.match(candi)
            if matches:
                #skip first one otherwise assign values
                if w_role != None:
                    ids.append(id)
                    w_roles.append(w_role)
                    w_turns.append(w_turn)
                    w_text.append(text)

                # deal with blank utterances
                text = ""

                # extract the groups
                w_role = matches.group(1)
                w_turn = matches.group(2)
            else:
                text = candi.strip()

    # make the dataframe
    df = pandas.DataFrame(zip(ids,w_roles,w_turns,w_text),columns=["id","w_role","w_turn","w_text"])

    #add a date
    df["loaded_date"] = f'{datetime.datetime.now().isoformat(timespec="seconds")}Z'

    # deal with empty strings
    df = df[~(df["w_text"]=="")]

    # unescape the html
    df["w_text"] = df["w_text"].apply(html.unescape)

    # write output
    output_filename = filename.replace(".csv","_output.csv")
    assert filename != output_filename
    df.to_csv(output_filename,header=True,index=False)
    print(f'Wrote to {output_filename}')
    print(df)

if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
