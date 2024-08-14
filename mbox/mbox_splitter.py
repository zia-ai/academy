"""
python mbox_splitter.py --filename <yourmboxfullpath>

This will read a very large mailbox file squentially from first prinicples.
It will write out to a directory in the same location as the filenmae named 
output or any --run_name within which will be folders by year, with subfolders by month
in each folder will be a json file per email.
It provides for progress reporting and keeps memory use low whilst relying on 
It provides email corruption tolerance and restartability 
(note where the last run got up to and provide that email)
Other options can be used to control restart or test limited runs but are not required
--max_lines        use to run on a sample head of the file
--max_emails       use to run on a sample of x emails of the file
--dummy            skips processing and just tests the directory looping
--count_increment  how often to give a count increment
--begin_line       which line of the file to start on, providing restartability.

@click.option('-n', '--max_emails', type=int, required=False, default=0,
              help='Max emails')
@click.option('-r', '--run_name', type=str, required=False, default="output",
              help='Run name otherwise defaults to output')
@click.option('-d', '--dummy', is_flag=True, required=False, default=False,
              help='Skip all processing')
@click.option('-c', '--count_increment', type=int, required=False, default=10000000,
              help='How often to give a report on process')
@click.option('-b', '--begin_line', type=int, required=False, default=0,
              help='How often to give a report on process')

https://www.loc.gov/preservation/digital/formats/fdd/fdd000383.shtml#:~:text=MBOX%20(sometimes%20known%20as%20Berkeley,the%20end%20of%20the%20file.


"""
# ******************************************************************************************************************120

# standard imports
from email.message import Message
from email import policy
import email
import os
import re
import time
import json
import collections
from dateutil import parser
import datetime
import bs4

# 3rd party imports
import click
import tiktoken

# custom imports

@click.command()
@click.option('-f', '--filename', type=str, required=True, help='Input File Path')
@click.option('-m', '--max_lines', type=int, required=False, default=0,
              help='Max lines to process')
@click.option('-n', '--max_emails', type=int, required=False, default=0,
              help='Max emails')
@click.option('-r', '--run_name', type=str, required=False, default="output",
              help='Run name otherwise defaults to output')
@click.option('-d', '--dummy', is_flag=True, required=False, default=False,
              help='Skip all processing')
@click.option('-c', '--count_increment', type=int, required=False, default=10000000,
              help='How often to give a report on process')
@click.option('-b', '--begin_line', type=int, required=False, default=0,
              help='How often to give a report on process')
def main(filename: str,
         max_lines: int,
         max_emails: int,
         run_name: str,
         dummy: bool,
         count_increment: int,
         begin_line: int) -> None:
    """Main Function"""

    # start perf logging
    loglist = []
    loglist = perf_log("Begin",loglist)

    # work out embeddings
    embeddings = tiktoken.encoding_for_model("gpt-4o")

    # compile re
    re_from_line = re.compile(f'^From ')
    re_file_name_removals = re.compile(r'[^A-Za-z0-9-_@\.]+')

    # output location
    input_path = os.path.split(filename)[0] # Head
    output_dir = os.path.join(input_path,run_name)
    if not os.path.isdir(output_dir):
        os.mkdir(output_dir)
    print(f'Writing to: {output_dir}')

    # cycle through file without examining all of it.
    file_in = open(filename,mode="r",encoding="utf8")

    # shouldn't matter than utf8 here
    # enca <takeout> --language=none
    # 7bit ASCII characters
    # CRLF line terminators

    current_email_str = ""
    count_emails = 0
    count_lines = 0
    count_unit = count_increment
    started = False

    # cycling through every line in the file
    for line in file_in:

        # exit if we've done enough
        if max_lines > 0 and count_lines > max_lines:
            break

        # don't start mid way through an email
        if count_lines >= begin_line and not dummy:

            # overall try and catch resetting the started in case there is something serious
            try:

                # see id it's the start od a new message
                if re_from_line.match(line):

                    # id not started, or restarted in the middel of message clear the incomplete message
                    if started == False:
                        current_email_str = ""
                        started = True
                        print("Started")
                        continue

                    # get the message object and parse it down.
                    msg = email.message_from_string(current_email_str,policy=policy.default)
                    custom = parse_message(msg,get_skeleton(),embeddings)

                    # workout output name
                    if custom["timestamp"] != '':
                        output_file_year = os.path.join(output_dir,custom["Year"])
                        if not os.path.isdir(output_file_year):
                            os.mkdir(output_file_year)
                        output_file_month = os.path.join(output_dir,custom["Year"],custom["Month"])
                        timestamp = custom["timestamp"]
                    else:
                        output_file_month = os.path.join(output_dir,"date_unknown")
                        timestamp = "unknown"
                    if not os.path.isdir(output_file_month):
                        os.mkdir(output_file_month)

                    try:
                        # From cleansed
                        if isinstance(custom["From"],list):
                            from_cleansed = '-'.join(custom["From"])
                        elif isinstance(custom["From"],str):
                            from_cleansed = custom["From"]
                        else:
                            raise RuntimeError("Unknown type of custom[From]")
                        from_cleansed = re_file_name_removals.sub("-",from_cleansed)
                        from_cleansed = from_cleansed.strip("-")
                        output_file_name = os.path.join(output_file_month,f'{timestamp}-{from_cleansed}.json')
                        if(len(output_file_name)>200):
                            print("SKIPPING")
                        else:
                            with open(output_file_name,mode="w",encoding="utf8") as file_out:
                                if(len(output_file_name)>200):
                                    raise RuntimeError("NameTooLong")
                                else:
                                    json.dump(custom,file_out,indent=2)
                                    # print(output_file_name)
                                file_out.close()
                    except Exception as e:
                        print(custom)
                        quit()
                        output_file_month = os.path.join(output_dir,"error")
                        if not os.path.isdir(output_file_month):
                            os.mkdir(output_file_month)
                        output_file_name = os.path.join(output_file_month,f'{datetime.datetime.now().isoformat()}-{from_cleansed}.json')
                        with open(output_file_name,mode="w",encoding="utf8") as file_out:
                            file_out.write(str(e))

                    current_email_str = ""
                    if max_emails > 0 and count_emails + 1 > max_emails:
                        print("Finishing")
                        break
                    count_emails = count_emails + 1
                else:
                    current_email_str = current_email_str + line
            except Exception as e:
                print("Foobar resetting started")
                print(str(e))
                print(custom)
                started = False

        #iterate counter
        count_lines = count_lines + 1

        # Log progress according to an interval
        if count_lines > count_unit:
            loglist = perf_log(count_unit, loglist)
            print(loglist[-1])
            count_unit = count_unit + count_increment


    print(f"Processed lines:  {max_lines}")
    print(f"Processed emails: {count_emails}")
    loglist = perf_log("Finish",loglist)
    for l in loglist:
        print(l)

    # 7 seconds per 10M lines
    # 2326 emails per 10M lines
    # ~ 4,300 lines per email
    file_in.close()

def parse_message(msg: Message,output_dict: collections.OrderedDict, embeddings) -> dict:
    """Parse and object and produce simplified dict"""

    # key fields for each message - only if exist
    fields_care_about = ['From','To','Cc','Date','Subject']

    for key in fields_care_about:
        if key in msg.keys():
            output_dict[key]=msg.get_all(key)[0]

    # content type to store field
    output_dict['parts'].append(msg.get_content_type())

    # workout a timestamp for filing
    if 'Date' in output_dict.keys() and output_dict['Date'] != '':
        if isinstance(output_dict['Date'],list):
            output_dict['timestamp'] = parser.parse(output_dict['Date'][0]).isoformat()
        elif isinstance(output_dict['Date'],str):
            output_dict['timestamp'] = parser.parse(output_dict['Date']).isoformat()
        else:
            raise RuntimeError(f'Unknown Date Format {type(output_dict["Date"])}')
        output_dict['Year'] = output_dict['timestamp'][0:4]
        output_dict['Month'] = output_dict['timestamp'][5:7]

    text_readable = ['text/plain']
    html_readable = ['text/html','text/x-amp-html']

    # file data and recursively call subobjects
    if not output_dict['found_content'] and msg.get_content_type() in text_readable:
        output_dict['content'] = build_content(output_dict,text=read_email_text(msg))
        output_dict['tokens'] = len(embeddings.encode(output_dict['content']))
        output_dict['found_content'] = True
    elif not output_dict['found_content'] and msg.get_content_type() in html_readable:
        output_dict['content'] = build_content(output_dict,text=get_html_text(read_email_text(msg)))
        output_dict['tokens'] = len(embeddings.encode(output_dict['content']))
        output_dict['found_content'] = True
    elif msg.get_content_type().startswith('multipart'):
        for part in msg.get_payload():
            parse_message(part,output_dict,embeddings)
    return output_dict

def build_content(output_dict: collections.OrderedDict, text: str) -> str:
    content = ''
    content = content + 'From: ' + output_dict['From'] + "\n"
    content = content + 'To: ' + output_dict['To'] + "\n"
    content = content + 'CC: ' + output_dict['Cc'] + "\n"
    content = content + 'timestamp: ' + output_dict['timestamp'] + "\n"
    content = content + 'Subject: ' + output_dict['Subject'] + "\n"
    content = content + 'Body: ' + text
    return content



def get_html_text(html):
    try:
        return bs4.BeautifulSoup(html, 'lxml').body.get_text(' ', strip=True)
    except AttributeError: # message contents empty
        return None

def read_email_text(msg: Message):
    try:
        # is obeying it's own type?
        text = msg.get_payload(decode=True).decode(msg.get_content_charset())
    except Exception as e:
        # No - see if windows solved it?
        try:
            text = msg.get_payload(decode=True).decode('windows-1252')
        except Exception as e:
            print("PAISLEY Error")
            print(e)
            text = ""
    return text

def get_skeleton() -> collections.OrderedDict:
    skeleton = collections.OrderedDict()
    skeleton['From'] = ''
    skeleton['To'] = ''
    skeleton['Cc'] = ''
    skeleton['Date'] = ''
    skeleton['Subject'] = ''
    skeleton['parts'] = []
    skeleton['content'] = ''
    skeleton['timestamp'] = ''
    skeleton['Year'] = ''
    skeleton['Month'] = ''
    skeleton['found_content'] = False
    skeleton['tokens'] = 0
    return skeleton

def perf_log(label: str, loglist: list) -> list:
    "Performance logging helper"
    now = time.perf_counter()
    if len(loglist) == 0:
        then = now
        start = now
    else:
        then = loglist[-1]['timestamp']
        start = loglist[0]['timestamp']
    log = {
        'label': label,
        'timestamp': now,
        'duration': now - then,
        'elapsed': now - start
    }
    loglist.append(log)
    return loglist

if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter

